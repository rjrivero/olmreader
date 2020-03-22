from zipfile import ZipFile
import logging
import sys
import argparse
from sqlalchemy import create_engine
import sqlalchemy.orm as orm

import attr
from lib import Email, Base


class Batch:
    """
    Keeps Batching items up to a given size,
    and running the action with each batch.
    """
    def __init__(self, batch_size, session, dry: bool = False):
        """Build a batch with the given size and action"""
        self._batch_size = batch_size
        self._batch = list()
        self._session = session
        self._total = 0
        self._dry = dry

    def add(self, item):
        """Add an item to the batch"""
        self._batch.append(item)
        if len(self._batch) >= self._batch_size:
            self._apply()

    def close(self):
        """Close the batch"""
        if len(self._batch) > 0:
            self._apply()
        self._session.close()

    def _apply(self):
        """Runs the action"""
        action = "Skipped"
        if not self._dry:
            action = "Saved"
            self._session.add_all(self._batch)
            self._session.commit()
        self._total += 1
        logging.info("%s batch %d with %d items", action, self._total,
                     len(self._batch))
        self._batch = list()


@attr.s(auto_attribs=True)
class Config:
    filename: str
    dbfile: str
    batch: int
    dry_run: bool

    @classmethod
    def load(cls):
        """Load config"""
        parser = argparse.ArgumentParser(
            description='Read OLM file and to SQLite database')
        parser.add_argument('filename',
                            metavar='filename',
                            type=str,
                            help='Path to OLM file name')
        parser.add_argument('dbfile',
                            metavar='dbfile',
                            type=str,
                            default='emails.db',
                            help='Name of database file')
        parser.add_argument('-b',
                            '--batch',
                            type=int,
                            default=100,
                            help='Size of the batch')
        parser.add_argument('-d',
                            '--dry',
                            action='store_true',
                            help='Dry-run (do not actually run)')
        args = parser.parse_args()
        return cls(filename=args.filename,
                   dbfile=args.dbfile,
                   batch=args.batch,
                   dry_run=args.dry)


def main():
    config = Config.load()
    engine = create_engine(f'sqlite:///{config.dbfile}', echo=False)
    Base.metadata.create_all(engine)
    smaker = orm.sessionmaker(bind=engine)

    with engine.connect():
        email_batch = Batch(config.batch, smaker(), dry=config.dry_run)
        with ZipFile(config.filename) as myzip:
            for info in myzip.infolist():
                if 'message_' in info.filename and not info.is_dir():
                    with myzip.open(info, mode='r') as msg:
                        email_batch.add(
                            Email.fromfile(info.filename, msg.read()))
            email_batch.close()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.DEBUG)
    main()