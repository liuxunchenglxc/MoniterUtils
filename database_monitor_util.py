from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import time
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import traceback


def create_db_engine(link_path: str):
    engine = create_engine(link_path,
                           pool_recycle=60,
                           pool_pre_ping=True,
                           pool_use_lifo=True,
                           echo_pool=True,
                           pool_size=1)
    return engine


class DBReader:
    def __init__(self, table_name: str, read_mode: str, value=None):
        self.table_name = table_name
        self.read_mode = read_mode
        self.value = value

    def read(self, session: Session, value_params=None):
        if self.read_mode == "last_line":
            sql = "select * from {}".format(self.table_name)
            rows = session.execute(sql).all()
            if len(rows) > 0:
                row = rows[-1]
                return [row]
        elif self.read_mode == "all_line":
            sql = "select * from {}".format(self.table_name)
            rows = session.execute(sql).all()
            return rows
        elif self.read_mode == "max_value":
            sql = "select max({}) from {}".format(self.value, self.table_name)
            row = session.execute(sql).first()
            if row is not None:
                return [row]
            else:
                return None
        elif self.read_mode == "condition":
            if value_params is not None:
                value = self.value.format(*value_params)
            else:
                value = self.value
            sql = "select * from {} {}".format(self.table_name, value)
            rows = session.execute(sql).all()
            return rows
        else:
            return None

    @staticmethod
    def rows_to_string(rows):
        return '\n'.join(['\t#\t'.join(str(i) for i in row) for row in rows])


class FileReader:
    def __init__(self, col_index: int, base_path: str):
        self.col_index = col_index
        self.base_path = base_path

    def read_file(self, row):
        if self.col_index < 0:
            return None
        file_name = row[self.col_index]
        try:
            with open(self.base_path + file_name, 'r') as f:
                lines = f.readlines()
        except BaseException:
            lines = None
        return lines

    @staticmethod
    def lines_to_string(lines, start=None, end=None):
        if start is None and end is None:
            return '\n'.join(lines)
        elif start is None:
            return '\n'.join(lines[:end])
        elif end is None:
            return '\n'.join(lines[start:])
        else:
            return '\n'.join(lines[start:end])


class DBListener:
    def __init__(self, table_name, moniter_name):
        self.link_err_time = -1
        self.table_name = table_name
        self.moniter_name = moniter_name

    def listen(self, engine: Engine):
        with Session(engine) as session:
            try:
                self.query(session)
                session.commit()
                self.link_err_time = -1
            except BaseException:
                exc = traceback.format_exc()
                print(exc)
                if self.link_err_time == -1:
                    self.link_err_time = time.time()
                else:
                    if time.time() - self.link_err_time > 600:
                        self.notify("{} DatabaseMonitor Lost Database Link",
                                    "Report by DBListener:\n{}".format(exc))
                        self.link_err_time = time.time()

    def notify(self, title: str, text: str):
        smtp = smtplib.SMTP()
        #################################
        #
        # Write Your Own E-mail Info
        #
        #################################
        smtp.connect("smtp.xxx.com", 25)
        smtp.login("xxxxxxxxxx@xxx.com", "smpt_key")
        from_addr = "xxxxxxxxxx@xxx.com"
        to_addr = "yyyyyyyyy@yyy.com"
        message = MIMEText(text, 'plain', 'utf-8')
        message['From'] = Header(self.moniter_name + " DatabaseMonitor", 'utf-8')
        message['To'] = Header("Administrator", 'utf-8')
        message['Subject'] = Header(title, 'utf-8')
        smtp.sendmail(from_addr, to_addr, message.as_string())

    def query(self, session: Session):
        pass


class RowAddListener(DBListener):
    def __init__(self, table_name: str, file_reader: FileReader = None, moniter_name=""):
        super(RowAddListener, self).__init__(table_name, moniter_name)
        self.db_reader = DBReader(table_name, "last_line")
        self.file_reader = file_reader
        self.row_count = -1

    def query(self, session: Session):
        sql = "select * from {}".format(self.table_name)
        rows = session.execute(sql).all()
        n = len(rows)
        if self.row_count < n and self.row_count != -1:
            add = n - self.row_count
            title = "Table '{}' RowAddListener".format(self.table_name)
            content = "The table '{}' is +{} of {} row(s).\n".format(self.table_name, add, n)
            content += "The last row:\n"
            try:
                rows = self.db_reader.read(session)
                content += DBReader.rows_to_string(rows)
                if self.file_reader is not None:
                    content += "\n\nThe file content of Column {}:\n".format(self.file_reader.col_index)
                    content += FileReader.lines_to_string(self.file_reader.read_file(rows[0]))
                self.notify(title, content)
            except BaseException:
                exc = traceback.format_exc()
                print(exc)
                self.notify("Error: " + title,
                            "Report by RowAddListener:\n{}".format(exc))
        self.row_count = n


class RowCountListener(DBListener):
    def __init__(self, table_name: str, count: int, moniter_name):
        super(RowCountListener, self).__init__(table_name, moniter_name)
        self.db_reader = DBReader(table_name, "all_line")
        self.count = count
        self.notified = False

    def query(self, session: Session):
        sql = "select * from {}".format(self.table_name)
        rows = session.execute(sql).all()
        do_notify = len(rows) >= self.count
        if do_notify and not self.notified:
            title = "Table '{}' RowCountListener".format(self.table_name)
            content = "The row count of table '{}' is {}.\n".format(self.table_name, self.count)
            content += "All rows:\n"
            content += DBReader.rows_to_string(self.db_reader.read(session))
            self.notify(title, content)
        self.notified = do_notify


class MaxValueListener(DBListener):
    def __init__(self, table_name: str, col_name: str, addition_db_reader: DBReader = None, moniter_name=None):
        super(MaxValueListener, self).__init__(table_name, moniter_name)
        self.db_reader = DBReader(table_name, "max_value", col_name)
        self.max_value = -1
        self.col_name = col_name
        self.addition_db_reader = addition_db_reader

    def query(self, session: Session):
        rows = self.db_reader.read(session)
        if rows is not None:
            max_value = rows[0][0]
            if self.max_value == -1 or max_value > self.max_value:
                self.max_value = max_value
                title = "Table '{}' MaxValueListener of '{}'".format(self.table_name, self.col_name)
                content = "The max value of '{}' is {}.\n".format(self.col_name, max_value)
                try:
                    if self.addition_db_reader is not None:
                        content += "The addition content:\n"
                        rows = self.addition_db_reader.read(session, (max_value,))
                        content += DBReader.rows_to_string(rows)
                    self.notify(title, content)
                except BaseException:
                    exc = traceback.format_exc()
                    print(exc)
                    self.notify("Error: " + title,
                                "Report by RowAddListener:\n{}".format(exc))


class DatabaseMonitor:
    def __init__(self, link_path: str):
        self.link_path = link_path
        self.listeners = []
        self.engine = create_db_engine(link_path)

    def add_listener(self, listener: DBListener):
        self.listeners.append(listener)

    def monite(self):
        while True:
            for listener in self.listeners:
                listener.listen(self.engine)
            time.sleep(60)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='DataBase Moniter')
    parser.add_argument('db_name', type=str, help='Name of DB, like DBName')
    parser.add_argument('dir_path', type=str, help='Path of project dir for read file, like /data/MyProject')
    parser.add_argument('moniter_name', type=str, help='Name of this moniter')
    args = parser.parse_args()
    ################################################
    #
    # Write Your Own DB Link
    #
    ################################################
    link_path = "your_db_protocal://your_db_link/" + args.db_name
    dm = DatabaseMonitor(link_path)
    ################################################
    #
    # Add Your Own Listener, Following is Example 
    #
    ################################################
    # dm.add_listener(RowAddListener("ds_bad_code", FileReader(1, args.dir_path + '/'), args.moniter_name))
    # dm.add_listener(RowAddListener("bad_code", FileReader(1, args.dir_path + '/'), args.moniter_name))
    # dm.add_listener(RowCountListener("ds_result", 10, args.moniter_name))
    # ad_cond_sql = "inner join generation on score.id=generation.id where generation.iteration={}-1 order by score.score desc"
    # dm.add_listener(
    #     MaxValueListener(
    #         "generation",
    #         "iteration",
    #         DBReader(
    #             "score",
    #             "condition",
    #             ad_cond_sql),
    #         args.moniter_name))
    dm.monite()
