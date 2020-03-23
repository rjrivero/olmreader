"""Models for importing messages and persisting to DB"""

from typing import (Optional, Iterator, Sequence, Mapping, Any, Type, TypeVar)
from html import unescape
from email.message import EmailMessage
from email.policy import SMTP
from email.utils import formatdate
import email
import itertools
import pytz
from sqlalchemy.ext.declarative import declarative_base
from dateutil.parser import isoparse

import sqlalchemy as sa
import sqlalchemy.orm as orm
import attr
import xmltodict

# pylint: disable=invalid-name
Base = declarative_base()


def many2many(base, tablename):
    """Address to message mappings"""
    return sa.Table(tablename, base.metadata,
                    sa.Column('address_id', None, sa.ForeignKey('address.id')),
                    sa.Column('message_id', None, sa.ForeignKey('message.id')),
                    sa.PrimaryKeyConstraint('address_id', 'message_id'),
                    sa.Index(f'idx_{tablename}_message', 'message_id'))


_email_from = many2many(Base, 'from')
_email_to = many2many(Base, 'to')
_email_cc = many2many(Base, 'cc')
_email_contact = many2many(Base, 'contact')


def alchemy(*args):
    """Decorator to add arguments to Table model"""
    def decorate(cls):
        """Add __table_args__"""
        setattr(cls, '__table_args__', args)
        return cls

    return decorate


def rel(table: str,
        populates: Optional[str] = None,
        secondary: Optional[sa.Table] = None):
    """Simplifies building a relation between tables"""
    kw = dict()
    if populates:
        kw['back_populates'] = populates
    if secondary is not None:
        kw['secondary'] = secondary
    return orm.relationship(table, **kw)


@alchemy(sa.UniqueConstraint('email', 'name', 'type'),
         sa.Index('idx_name', 'name'))
class Address(Base):
    """Represents an email address"""
    __tablename__ = "address"

    id = sa.Column(sa.Integer, primary_key=True)
    type = sa.Column(sa.Integer, nullable=True)
    email = sa.Column(sa.String(320), nullable=True)
    name = sa.Column(sa.String(320), nullable=True)
    messages = rel('Email', 'contacts', _email_contact)

    # pylint: disable=dangerous-default-value
    @classmethod
    def fromdict(cls, email_address, cache=dict()):
        """Build an Address from a OML Address dict"""
        name = email_address.get('@OPFContactEmailAddressName', None)
        if name is not None:
            # Remove leading and trailing quotes
            name = name.strip('\'"').replace('"', '').strip() or None
        data = cls(id=len(cache),
                   type=email_address.get('@OPFContactEmailAddressType', 0),
                   name=name,
                   email=email_address.get('@OPFContactEmailAddressAddress',
                                           None))
        key = data.key()
        # TODO: Should we prepopulate the cache?
        addr = cache.get(key, None)
        if addr is None:
            # Keep addresses in a cache, to avoid
            # creating several items with same unique key
            # constraint.
            addr = data
            cache[key] = addr
        return addr

    def key(self):
        """Return unique key built from Address"""
        return (self.email, self.name, self.type)

    def rfc2822(self):
        """Return the address as a display name from RFC2822"""
        if not self.name:
            return self.email
        return f'"{self.name}" <{self.email}>'


class Attachment(Base):
    """Attachment information (not actual data)"""
    __tablename__ = 'attachment'

    id = sa.Column(sa.Integer, sa.Sequence('seq_attachment'), primary_key=True)
    path = sa.Column(sa.String(1024), nullable=False)
    ext = sa.Column(sa.String(8), nullable=True)
    size = sa.Column(sa.Integer, nullable=True)
    type = sa.Column(sa.String(128), nullable=True)
    name = sa.Column(sa.String(128), nullable=True)
    message_id = sa.Column(None, sa.ForeignKey('message.id'))
    message = rel('Email', populates='attachments')

    def key(self):
        """Return unique key for attachment (path)"""
        return self.path

    @classmethod
    def fromdict(cls, attachment):
        """Build an Attachment from a OML Attachment dict"""
        size = attachment.get('@OPFAttachmentContentFileSize', None)
        attach = cls(
            ext=attachment.get('@OPFAttachmentContentExtension', None),
            size=float(size.replace(',', '.')) if size is not None else None,
            type=attachment.get('@OPFAttachmentContentType', None),
            name=attachment.get('@OPFAttachmentName', None),
            path=attachment.get('@OPFAttachmentURL', None))
        return attach


D = Mapping[str, Any]
T = TypeVar('T', Address, Attachment)


@attr.s(auto_attribs=True, auto_exc=True)
class ParseError(Exception):
    """Error raised when parsing XML fails"""
    # Missing key
    key: KeyError
    # Attribute where the key is missing
    attribute: Optional[D]
    # Email where the attibute comes from
    email: Optional[D]
    # XML where the email comes from
    xml: Optional[str]


@alchemy(sa.Index('idx_label', 'label'), sa.Index('idx_sentTime', 'sentTime'),
         sa.Index('idx_receivedTime', 'receivedTime'))
class Email(Base):
    """Email database format"""
    __tablename__ = 'message'

    id = sa.Column(sa.Integer, sa.Sequence('seq_message'), primary_key=True)
    path = sa.Column(sa.String(1024), unique=True)
    account = sa.Column(sa.String(320), nullable=False)
    label = sa.Column(sa.String(320), nullable=False)
    isOutgoingMeetingResponse = sa.Column(sa.Boolean, nullable=False)
    isOutgoing = sa.Column(sa.Boolean, nullable=False)
    isCalendarMessage = sa.Column(sa.Boolean, nullable=False)
    hasHTML = sa.Column(sa.Boolean, nullable=False)
    hasRichText = sa.Column(sa.Boolean, nullable=False)
    # SentTime and receivedTime are stored as UTC timestamps,
    # to avoid limitations with SQLite
    sentTime = sa.Column(sa.BigInteger, nullable=True)
    receivedTime = sa.Column(sa.BigInteger, nullable=True)
    displayTo = sa.Column(sa.String(16), nullable=True)
    subject = sa.Column(sa.String(512), nullable=True)
    preview = sa.Column(sa.Text, nullable=True)
    body = sa.Column(sa.Text, nullable=True)
    fromAddresses = rel('Address', secondary=_email_from)
    toAddresses = rel('Address', secondary=_email_to)
    ccAddresses = rel('Address', secondary=_email_cc)
    contacts = rel('Address', 'messages', _email_contact)
    attachments = rel('Attachment', populates='message')

    @staticmethod
    def unique(items: Optional[Iterator[T]]) -> Sequence[T]:
        """Filters out non unique items"""
        visited = dict()
        if items is not None:
            for item in items:
                key = item.key()
                if key is not None:
                    visited[key] = item
        return list(visited.values())

    @staticmethod
    def _contacts(*lists: Optional[Sequence[Address]]) -> Sequence[Address]:
        """Iterates over all addresses (sender, recipients, cc's)"""
        return Email.unique(
            itertools.chain(*(item for item in lists if item is not None)))

    @staticmethod
    def _text(msg: D, attrib: str) -> Optional[str]:
        """Retrieve text from email attibute"""
        val = msg.get(attrib, None)
        return None if val is None else val.get('#text', None)

    @staticmethod
    def _html(msg: D, attrib: str) -> Optional[str]:
        """Get html text from an attibute"""
        val = Email._text(msg, attrib)
        return None if val is None else unescape(val)

    @staticmethod
    def _bool(msg: D, attrib: str) -> Optional[bool]:
        """Get boolean from attribute"""
        val = Email._text(msg, attrib)
        return None if val is None else (float(val) > 0)

    @staticmethod
    def _timestamp(msg: D, attrib: str) -> Optional[int]:
        """Get boolean from attribute"""
        val = Email._text(msg, attrib)
        if val is None:
            return None
        # SQLite does not have a native datetime format,
        # so we set a timestamp.
        tstamp = isoparse(val).astimezone(pytz.utc)
        return int(tstamp.timestamp())

    @staticmethod
    def _list(item: D, attribute, cls: Type[T]) -> Optional[Sequence[T]]:
        """
        Build a list of items of the given cls
        from the given item's attribute
        """
        try:
            if item is None:
                return list()
            values = item[attribute]
            if hasattr(values, 'items'):  # dictionary
                values = (values, )
            return Email.unique(cls.fromdict(value) for value in values)
        except KeyError as err:
            raise ParseError(key=err, attribute=item, email=None, xml=None)

    @staticmethod
    def _addresses(msg: D, attrib: str) -> Optional[Sequence[Address]]:
        """Get list of addresses from attribute"""
        return Email._list(msg.get(attrib, None), 'emailAddress', Address)

    @staticmethod
    def _attachments(msg: D, attrib: str) -> Optional[Sequence[Attachment]]:
        """Get list of attachments from attribute"""
        return Email._list(msg.get(attrib, None), 'messageAttachment',
                           Attachment)

    @staticmethod
    def _address(msg: D, attrib: str) -> Optional[Address]:
        """Get address from attribute"""
        value = msg.get(attrib, None)
        return None if value is None else Address.fromdict(
            value['emailAddress'])

    def message(self, policy=SMTP) -> EmailMessage:
        """Formats the current message as an EmailMessage"""
        msg = email.message.EmailMessage(policy)
        msg['Subject'] = self.subject or ""
        for field, seq in (('From', self.fromAddresses),
                           ('To', self.toAddresses), ('CC', self.ccAddresses)):
            value = ""
            if seq is not None:
                value = ",".join(addr.rfc2822() for addr in seq
                                 if addr.email or addr.name)
            msg.add_header(field, value)
        if self.sentTime is not None:
            msg.add_header('Date', formatdate(self.sentTime))
        if self.body is not None:
            if self.hasHTML:
                msg.add_header('Content-Type', 'text/html')
            elif self.hasHTML:
                msg.add_header('Content-Type', 'text/rtf')
            else:
                msg.add_header('Content-Type', 'text/plain')
            msg.set_param('encoding', 'utf-8')
            msg.set_param('charset', 'utf-8')
            msg.set_payload(self.body, 'utf-8')
        return msg

    @classmethod
    def fromdict(cls, path, msg):
        """Build email message from dictionary"""
        parts = path.split('/')
        try:
            # 'From' header comes as fromAddresses or
            # senderAddress, some messages have fromAddresses
            # and some don't.
            fromAddresses = Email._addresses(msg,
                                             'OPFMessageCopyFromAddresses')
            senderAddress = Email._address(msg, 'OPFMessageCopySenderAddress')
            if senderAddress is not None:
                fromAddresses.append(senderAddress)
                fromAddresses = Email.unique(fromAddresses)
            toAddresses = Email._addresses(msg, 'OPFMessageCopyToAddresses')
            ccAddresses = Email._addresses(msg, 'OPFMessageCopyCCAddresses')
            contacts = Email._contacts(fromAddresses, toAddresses, ccAddresses)
            msg = cls(
                path=path,
                account=parts[1],
                label='/'.join(parts[3:-1]),
                isOutgoingMeetingResponse=Email._bool(
                    msg, 'OPFMessageIsOutgoingMeetingResponse'),
                isOutgoing=Email._bool(msg, 'OPFMessageIsOutgoing') or False,
                isCalendarMessage=Email._bool(
                    msg, 'OPFMessageIsCalendarMessage') or False,
                hasHTML=Email._bool(msg, 'OPFMessageGetHasHTML') or False,
                hasRichText=Email._bool(msg, 'OPFMessageGetHasRichText')
                or False,
                toAddresses=toAddresses,
                preview=Email._text(msg, 'OPFMessageCopyPreview'),
                receivedTime=Email._timestamp(msg,
                                              'OPFMessageCopyReceivedTime'),
                fromAddresses=fromAddresses,
                sentTime=Email._timestamp(msg, 'OPFMessageCopySentTime'),
                subject=Email._text(msg, 'OPFMessageCopySubject'),
                ccAddresses=ccAddresses,
                contacts=contacts,
                displayTo=Email._text(msg, 'OPFMessageCopyDisplayTo'),
                attachments=Email._attachments(msg,
                                               'OPFMessageCopyAttachmentList'),
                body=Email._html(msg, 'OPFMessageCopyBody'))
            return msg
        except ParseError as err:
            err.email = msg
            raise err
        except KeyError as err:
            raise ParseError(key=err, email=msg)

    @classmethod
    def fromfile(cls, path: str, contents: str):
        """Build email message from XML file"""
        try:
            xml = xmltodict.parse(contents)
            return cls.fromdict(path, xml['emails']['email'])
        except ParseError as err:
            err.xml = contents
            raise err
