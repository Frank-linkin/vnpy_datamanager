from functools import partial
import re
from datetime import datetime, timedelta

from vnpy.trader.ui import QtWidgets, QtCore
from vnpy.trader.engine import MainEngine, EventEngine
from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.object import BarData
from vnpy.trader.database import DB_TZ
from vnpy.trader.utility import available_timezones

from ..engine import APP_NAME, ManagerEngine, BarOverview, BatchDownloadTask


INTERVAL_NAME_MAP = {
    Interval.MINUTE: "分钟线",
    Interval.HOUR: "小时线",
    Interval.DAILY: "日线",
}

DIVIDEND_TYPE_CHOICES: list[tuple[str, str]] = [
    ("不复权", "none"),
    ("前复权", "front"),
    ("后复权", "back"),
    ("等比前复权", "front_ratio"),
    ("等比后复权", "back_ratio"),
]


class ManagerWidget(QtWidgets.QWidget):
    """"""

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__()

        self.engine: ManagerEngine = main_engine.get_engine(APP_NAME)

        self.init_ui()

    def init_ui(self) -> None:
        """"""
        self.setWindowTitle("数据管理")

        self.init_tree()
        self.init_table()

        refresh_button: QtWidgets.QPushButton = QtWidgets.QPushButton("刷新")
        refresh_button.clicked.connect(self.refresh_tree)

        import_button: QtWidgets.QPushButton = QtWidgets.QPushButton("导入数据")
        import_button.clicked.connect(self.import_data)

        update_button: QtWidgets.QPushButton = QtWidgets.QPushButton("更新数据")
        update_button.clicked.connect(self.update_data)

        download_button: QtWidgets.QPushButton = QtWidgets.QPushButton("下载数据")
        download_button.clicked.connect(self.download_data)
        batch_download_button: QtWidgets.QPushButton = QtWidgets.QPushButton("批量下载")
        batch_download_button.clicked.connect(self.batch_download_data)

        hbox1: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox1.addWidget(refresh_button)
        hbox1.addStretch()
        hbox1.addWidget(import_button)
        hbox1.addWidget(update_button)
        hbox1.addWidget(download_button)
        hbox1.addWidget(batch_download_button)

        hbox2: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        hbox2.addWidget(self.tree)
        hbox2.addWidget(self.table)

        vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        vbox.addLayout(hbox1)
        vbox.addLayout(hbox2)

        self.setLayout(vbox)

    def init_tree(self) -> None:
        """"""
        labels: list = [
            "数据",
            "本地代码",
            "代码",
            "交易所",
            "数据量",
            "开始时间",
            "结束时间",
            "",
            "",
            ""
        ]

        self.tree: QtWidgets.QTreeWidget = QtWidgets.QTreeWidget()
        self.tree.setColumnCount(len(labels))
        self.tree.setHeaderLabels(labels)

    def init_table(self) -> None:
        """"""
        labels: list = [
            "时间",
            "开盘价",
            "最高价",
            "最低价",
            "收盘价",
            "成交量",
            "成交额",
            "持仓量"
        ]

        self.table: QtWidgets.QTableWidget = QtWidgets.QTableWidget()
        self.table.setColumnCount(len(labels))
        self.table.setHorizontalHeaderLabels(labels)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.ResizeMode.ResizeToContents
        )

    def refresh_tree(self) -> None:
        """"""
        self.tree.clear()

        # 初始化节点缓存字典
        interval_childs: dict[Interval, QtWidgets.QTreeWidgetItem] = {}
        exchange_childs: dict[tuple[Interval, Exchange], QtWidgets.QTreeWidgetItem] = {}

        # 查询数据汇总，并基于合约代码进行排序
        overviews: list[BarOverview] = self.engine.get_bar_overview()
        overviews.sort(key=lambda x: x.symbol)

        # 添加数据周期节点
        for interval in [Interval.MINUTE, Interval.HOUR, Interval.DAILY]:
            interval_child = QtWidgets.QTreeWidgetItem()
            interval_childs[interval] = interval_child

            interval_name: str = INTERVAL_NAME_MAP[interval]
            interval_child.setText(0, interval_name)

        # 遍历添加数据节点
        for overview in overviews:
            # 获取交易所节点
            key: tuple = (overview.interval, overview.exchange)
            exchange_child: QtWidgets.QTreeWidgetItem = exchange_childs.get(key, None)

            if not exchange_child:
                interval_child = interval_childs[overview.interval]

                exchange_child = QtWidgets.QTreeWidgetItem(interval_child)
                exchange_child.setText(0, overview.exchange.value)

                exchange_childs[key] = exchange_child

            #  创建数据节点
            item = QtWidgets.QTreeWidgetItem(exchange_child)

            item.setText(1, f"{overview.symbol}.{overview.exchange.value}")
            item.setText(2, overview.symbol)
            item.setText(3, overview.exchange.value)
            item.setText(4, str(overview.count))
            item.setText(5, overview.start.strftime("%Y-%m-%d %H:%M:%S"))
            item.setText(6, overview.end.strftime("%Y-%m-%d %H:%M:%S"))

            output_button: QtWidgets.QPushButton = QtWidgets.QPushButton("导出")
            output_func = partial(
                self.output_data,
                overview.symbol,
                overview.exchange,
                overview.interval,
                overview.start,
                overview.end
            )
            output_button.clicked.connect(output_func)

            show_button: QtWidgets.QPushButton = QtWidgets.QPushButton("查看")
            show_func = partial(
                self.show_data,
                overview.symbol,
                overview.exchange,
                overview.interval,
                overview.start,
                overview.end
            )
            show_button.clicked.connect(show_func)

            delete_button: QtWidgets.QPushButton = QtWidgets.QPushButton("删除")
            delete_func = partial(
                self.delete_data,
                overview.symbol,
                overview.exchange,
                overview.interval
            )
            delete_button.clicked.connect(delete_func)

            self.tree.setItemWidget(item, 7, show_button)
            self.tree.setItemWidget(item, 8, output_button)
            self.tree.setItemWidget(item, 9, delete_button)

        # 展开顶层节点
        self.tree.addTopLevelItems(list(interval_childs.values()))

        for interval_child in interval_childs.values():
            interval_child.setExpanded(True)

    def import_data(self) -> None:
        """"""
        dialog: ImportDialog = ImportDialog()
        n: int = dialog.exec_()
        if n != dialog.DialogCode.Accepted:
            return

        file_path: str = dialog.file_edit.text()
        symbol: str = dialog.symbol_edit.text()
        exchange = dialog.exchange_combo.currentData()
        interval = dialog.interval_combo.currentData()
        tz_name: str = dialog.tz_combo.currentText()
        datetime_head: str = dialog.datetime_edit.text()
        open_head: str = dialog.open_edit.text()
        low_head: str = dialog.low_edit.text()
        high_head: str = dialog.high_edit.text()
        close_head: str = dialog.close_edit.text()
        volume_head: str = dialog.volume_edit.text()
        turnover_head: str = dialog.turnover_edit.text()
        open_interest_head: str = dialog.open_interest_edit.text()
        datetime_format: str = dialog.format_edit.text()

        start, end, count = self.engine.import_data_from_csv(
            file_path,
            symbol,
            exchange,
            interval,
            tz_name,
            datetime_head,
            open_head,
            high_head,
            low_head,
            close_head,
            volume_head,
            turnover_head,
            open_interest_head,
            datetime_format
        )

        msg: str = f"\
        CSV载入成功\n\
        代码：{symbol}\n\
        交易所：{exchange.value}\n\
        周期：{interval.value}\n\
        起始：{start}\n\
        结束：{end}\n\
        总数量：{count}\n\
        "
        QtWidgets.QMessageBox.information(self, "载入成功！", msg)

    def output_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> None:
        """"""
        # Get output date range
        dialog: DateRangeDialog = DateRangeDialog(start, end)
        n: int = dialog.exec_()
        if n != dialog.DialogCode.Accepted:
            return
        start, end = dialog.get_date_range()

        # Get output file path
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self,
            "导出数据",
            "",
            "CSV(*.csv)"
        )
        if not path:
            return

        result: bool = self.engine.output_data_to_csv(
            path,
            symbol,
            exchange,
            interval,
            start,
            end
        )

        if not result:
            QtWidgets.QMessageBox.warning(
                self,
                "导出失败！",
                "该文件已在其他程序中打开，请关闭相关程序后再尝试导出数据。"
            )

    def show_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> None:
        """"""
        # Get output date range
        dialog: DateRangeDialog = DateRangeDialog(start, end)
        n: int = dialog.exec_()
        if n != dialog.DialogCode.Accepted:
            return
        start, end = dialog.get_date_range()

        bars: list[BarData] = self.engine.load_bar_data(
            symbol,
            exchange,
            interval,
            start,
            end
        )

        self.table.setRowCount(0)
        self.table.setRowCount(len(bars))

        for row, bar in enumerate(bars):
            self.table.setItem(row, 0, DataCell(bar.datetime.strftime("%Y-%m-%d %H:%M:%S")))
            self.table.setItem(row, 1, DataCell(str(bar.open_price)))
            self.table.setItem(row, 2, DataCell(str(bar.high_price)))
            self.table.setItem(row, 3, DataCell(str(bar.low_price)))
            self.table.setItem(row, 4, DataCell(str(bar.close_price)))
            self.table.setItem(row, 5, DataCell(str(bar.volume)))
            self.table.setItem(row, 6, DataCell(str(bar.turnover)))
            self.table.setItem(row, 7, DataCell(str(bar.open_interest)))

    def delete_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> None:
        """"""
        n = QtWidgets.QMessageBox.warning(
            self,
            "删除确认",
            f"请确认是否要删除{symbol} {exchange.value} {interval.value}的全部数据",
            QtWidgets.QMessageBox.Ok,
            QtWidgets.QMessageBox.Cancel
        )

        if n == QtWidgets.QMessageBox.Cancel:
            return

        count: int = self.engine.delete_bar_data(
            symbol,
            exchange,
            interval
        )

        QtWidgets.QMessageBox.information(
            self,
            "删除成功",
            f"已删除{symbol} {exchange.value} {interval.value}共计{count}条数据",
            QtWidgets.QMessageBox.Ok
        )

    def update_data(self) -> None:
        """"""
        overviews: list[BarOverview] = self.engine.get_bar_overview()
        total: int = len(overviews)
        count: int = 0

        dialog: QtWidgets.QProgressDialog = QtWidgets.QProgressDialog(
            "历史数据更新中",
            "取消",
            0,
            100
        )
        dialog.setWindowTitle("更新进度")
        dialog.setWindowModality(QtCore.Qt.WindowModal)
        dialog.setValue(0)

        for overview in overviews:
            if dialog.wasCanceled():
                break

            self.engine.download_bar_data(
                overview.symbol,
                overview.exchange,
                overview.interval,
                overview.end,
                self.output
            )
            count += 1
            progress = int(round(count / total * 100, 0))
            dialog.setValue(progress)

        dialog.close()

    def download_data(self) -> None:
        """"""
        dialog: DownloadDialog = DownloadDialog(self.engine)
        dialog.exec_()

    def batch_download_data(self) -> None:
        """
        Open batch download dialog.
        打开批量下载对话框。
        """
        dialog: BatchDownloadDialog = BatchDownloadDialog(self.engine, self)
        dialog.exec_()

    def show(self) -> None:
        """"""
        self.showMaximized()

    def output(self, msg: str) -> None:
        """输出下载过程中的日志"""
        QtWidgets.QMessageBox.warning(
            self,
            "数据下载",
            msg,
            QtWidgets.QMessageBox.Ok,
            QtWidgets.QMessageBox.Ok,
        )


class DataCell(QtWidgets.QTableWidgetItem):
    """"""

    def __init__(self, text: str = "") -> None:
        super().__init__(text)

        self.setTextAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)


class DateRangeDialog(QtWidgets.QDialog):
    """"""

    def __init__(self, start: datetime, end: datetime, parent: QtWidgets.QWidget | None = None) -> None:
        """"""
        super().__init__(parent)

        self.setWindowTitle("选择数据区间")

        self.start_edit: QtWidgets.QDateEdit = QtWidgets.QDateEdit(
            QtCore.QDate(
                start.year,
                start.month,
                start.day + 1
            )
        )
        self.end_edit: QtWidgets.QDateEdit = QtWidgets.QDateEdit(
            QtCore.QDate(
                end.year,
                end.month,
                end.day + 1
            )
        )

        button: QtWidgets.QPushButton = QtWidgets.QPushButton("确定")
        button.clicked.connect(self.accept)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form.addRow("开始时间", self.start_edit)
        form.addRow("结束时间", self.end_edit)
        form.addRow(button)

        self.setLayout(form)

    def get_date_range(self) -> tuple[datetime, datetime]:
        """"""
        start = self.start_edit.dateTime().toPython()
        end = self.end_edit.dateTime().toPython() + timedelta(days=1)
        return start, end


class ImportDialog(QtWidgets.QDialog):
    """"""

    def __init__(self, parent: QtWidgets.QWidget | None = None) -> None:
        """"""
        super().__init__()

        self.setWindowTitle("从CSV文件导入数据")
        self.setFixedWidth(300)

        self.setWindowFlags(
            (self.windowFlags() | QtCore.Qt.CustomizeWindowHint)
            & ~QtCore.Qt.WindowMaximizeButtonHint)

        file_button: QtWidgets.QPushButton = QtWidgets.QPushButton("选择文件")
        file_button.clicked.connect(self.select_file)

        load_button: QtWidgets.QPushButton = QtWidgets.QPushButton("确定")
        load_button.clicked.connect(self.accept)

        self.file_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit()
        self.symbol_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit()

        self.exchange_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        for i in Exchange:
            self.exchange_combo.addItem(str(i.name), i)

        self.interval_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        for i in Interval:
            if i != Interval.TICK:
                self.interval_combo.addItem(str(i.name), i)

        self.tz_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        self.tz_combo.addItems(available_timezones())
        self.tz_combo.setCurrentIndex(self.tz_combo.findText("Asia/Shanghai"))

        self.datetime_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("datetime")
        self.open_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("open")
        self.high_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("high")
        self.low_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("low")
        self.close_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("close")
        self.volume_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("volume")
        self.turnover_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("turnover")
        self.open_interest_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("open_interest")

        self.format_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit("%Y-%m-%d %H:%M:%S")

        info_label: QtWidgets.QLabel = QtWidgets.QLabel("合约信息")
        info_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

        head_label: QtWidgets.QLabel = QtWidgets.QLabel("表头信息")
        head_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

        format_label: QtWidgets.QLabel = QtWidgets.QLabel("格式信息")
        format_label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form.addRow(file_button, self.file_edit)
        form.addRow(QtWidgets.QLabel())
        form.addRow(info_label)
        form.addRow("代码", self.symbol_edit)
        form.addRow("交易所", self.exchange_combo)
        form.addRow("周期", self.interval_combo)
        form.addRow("时区", self.tz_combo)
        form.addRow(QtWidgets.QLabel())
        form.addRow(head_label)
        form.addRow("时间戳", self.datetime_edit)
        form.addRow("开盘价", self.open_edit)
        form.addRow("最高价", self.high_edit)
        form.addRow("最低价", self.low_edit)
        form.addRow("收盘价", self.close_edit)
        form.addRow("成交量", self.volume_edit)
        form.addRow("成交额", self.turnover_edit)
        form.addRow("持仓量", self.open_interest_edit)
        form.addRow(QtWidgets.QLabel())
        form.addRow(format_label)
        form.addRow("时间格式", self.format_edit)
        form.addRow(QtWidgets.QLabel())
        form.addRow(load_button)

        self.setLayout(form)

    def select_file(self) -> None:
        """"""
        result: str = QtWidgets.QFileDialog.getOpenFileName(
            self, filter="CSV (*.csv)")
        filename: str = result[0]
        if filename:
            self.file_edit.setText(filename)


class BatchDownloadDialog(QtWidgets.QDialog):
    """
    Batch download dialog with in-memory task table.
    批量下载对话框（任务仅存内存）。
    """

    headers: list[str] = [
        "ID",
        "代码",
        "交易所",
        "周期",
        "复权",
        "开始",
        "结束",
        "状态",
        "错误",
    ]

    def __init__(self, engine: ManagerEngine, parent: QtWidgets.QWidget | None = None) -> None:
        """
        Initialize dialog and auto-refresh timer.
        初始化对话框并启动自动刷新定时器。
        """
        super().__init__(parent)

        self.engine: ManagerEngine = engine
        self.setWindowTitle("批量下载")
        self.resize(1100, 600)

        self.timer: QtCore.QTimer = QtCore.QTimer(self)
        self.timer.setInterval(500)
        self.timer.timeout.connect(self.refresh_table)

        self.init_ui()
        self.refresh_table()
        self.timer.start()

    def init_ui(self) -> None:
        """
        Build input form, shortcut buttons and task table.
        构建参数输入区、快捷按钮和任务表格。
        """
        self.symbol_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit()

        self.exchange_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        for exchange in Exchange:
            self.exchange_combo.addItem(exchange.name, exchange)

        self.interval_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        for interval in Interval:
            self.interval_combo.addItem(interval.name, interval)

        self.dividend_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        for name, value in DIVIDEND_TYPE_CHOICES:
            self.dividend_combo.addItem(name, value)

        end_dt: datetime = datetime.now()
        start_dt: datetime = end_dt - timedelta(days=365)

        self.start_date_edit: QtWidgets.QDateEdit = QtWidgets.QDateEdit(
            QtCore.QDate(start_dt.year, start_dt.month, start_dt.day)
        )
        self.start_date_edit.setCalendarPopup(True)

        self.end_date_edit: QtWidgets.QDateEdit = QtWidgets.QDateEdit(
            QtCore.QDate(end_dt.year, end_dt.month, end_dt.day)
        )
        self.end_date_edit.setCalendarPopup(True)

        # Start date shortcuts: from small month range to large month range.
        # 开始日期快捷按钮：按月份从小到大排列。
        start_shortcut_widget: QtWidgets.QWidget = QtWidgets.QWidget()
        start_shortcut_hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        start_shortcut_hbox.setContentsMargins(0, 0, 0, 0)

        start_3m_button: QtWidgets.QPushButton = QtWidgets.QPushButton("前3月")
        start_3m_button.clicked.connect(lambda: self.set_start_date_by_symbol(3))
        start_shortcut_hbox.addWidget(start_3m_button)

        start_6m_button: QtWidgets.QPushButton = QtWidgets.QPushButton("前6月")
        start_6m_button.clicked.connect(lambda: self.set_start_date_by_symbol(6))
        start_shortcut_hbox.addWidget(start_6m_button)

        start_9m_button: QtWidgets.QPushButton = QtWidgets.QPushButton("前9月")
        start_9m_button.clicked.connect(lambda: self.set_start_date_by_symbol(9))
        start_shortcut_hbox.addWidget(start_9m_button)

        start_12m_button: QtWidgets.QPushButton = QtWidgets.QPushButton("前12月")
        start_12m_button.clicked.connect(lambda: self.set_start_date_by_symbol(12))
        start_shortcut_hbox.addWidget(start_12m_button)

        start_shortcut_widget.setLayout(start_shortcut_hbox)

        # End date shortcuts: add N months based on selected start date.
        # 结束日期快捷按钮：基于开始日期向后推N个月。
        end_shortcut_widget: QtWidgets.QWidget = QtWidgets.QWidget()
        end_shortcut_hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        end_shortcut_hbox.setContentsMargins(0, 0, 0, 0)

        end_3m_button: QtWidgets.QPushButton = QtWidgets.QPushButton("+3月")
        end_3m_button.clicked.connect(lambda: self.set_end_date_from_start(3))
        end_shortcut_hbox.addWidget(end_3m_button)

        end_6m_button: QtWidgets.QPushButton = QtWidgets.QPushButton("+6月")
        end_6m_button.clicked.connect(lambda: self.set_end_date_from_start(6))
        end_shortcut_hbox.addWidget(end_6m_button)

        end_9m_button: QtWidgets.QPushButton = QtWidgets.QPushButton("+9月")
        end_9m_button.clicked.connect(lambda: self.set_end_date_from_start(9))
        end_shortcut_hbox.addWidget(end_9m_button)

        end_12m_button: QtWidgets.QPushButton = QtWidgets.QPushButton("+12月")
        end_12m_button.clicked.connect(lambda: self.set_end_date_from_start(12))
        end_shortcut_hbox.addWidget(end_12m_button)

        end_shortcut_widget.setLayout(end_shortcut_hbox)

        add_button: QtWidgets.QPushButton = QtWidgets.QPushButton("添加任务")
        add_button.clicked.connect(self.add_task)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form.addRow("代码", self.symbol_edit)
        form.addRow("交易所", self.exchange_combo)
        form.addRow("周期", self.interval_combo)
        form.addRow("复权参数", self.dividend_combo)
        form.addRow("开始日期", self.start_date_edit)
        form.addRow("", start_shortcut_widget)
        form.addRow("结束日期", self.end_date_edit)
        form.addRow("", end_shortcut_widget)
        form.addRow(add_button)

        form_widget: QtWidgets.QWidget = QtWidgets.QWidget()
        form_widget.setLayout(form)

        # Table mirrors engine in-memory task snapshots.
        # 表格展示引擎中的内存任务快照。
        self.table: QtWidgets.QTableWidget = QtWidgets.QTableWidget()
        self.table.setColumnCount(len(self.headers))
        self.table.setHorizontalHeaderLabels(self.headers)
        self.table.verticalHeader().setVisible(False)
        self.table.horizontalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.ResizeMode.ResizeToContents
        )
        self.table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)

        self.start_button: QtWidgets.QPushButton = QtWidgets.QPushButton("开始批量下载")
        self.start_button.clicked.connect(self.start_batch_download)

        remove_button: QtWidgets.QPushButton = QtWidgets.QPushButton("删除选中")
        remove_button.clicked.connect(self.remove_selected_tasks)

        clear_button: QtWidgets.QPushButton = QtWidgets.QPushButton("清空已完成")
        clear_button.clicked.connect(self.clear_completed_tasks)

        close_button: QtWidgets.QPushButton = QtWidgets.QPushButton("关闭")
        close_button.clicked.connect(self.accept)

        controls_hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        controls_hbox.addWidget(self.start_button)
        controls_hbox.addWidget(remove_button)
        controls_hbox.addWidget(clear_button)
        controls_hbox.addStretch()
        controls_hbox.addWidget(close_button)

        right_vbox: QtWidgets.QVBoxLayout = QtWidgets.QVBoxLayout()
        right_vbox.addWidget(self.table)
        right_vbox.addLayout(controls_hbox)

        body_hbox: QtWidgets.QHBoxLayout = QtWidgets.QHBoxLayout()
        body_hbox.addWidget(form_widget, 1)
        body_hbox.addLayout(right_vbox, 3)

        self.setLayout(body_hbox)

    def set_start_date_by_symbol(self, months: int) -> None:
        """
        Infer contract year/month from symbol and move start date backward.
        根据合约代码推断年月，并回推开始日期。
        """
        symbol: str = self.symbol_edit.text().strip()
        if not symbol:
            return

        exchange: Exchange = self.exchange_combo.currentData()
        contract_ym: tuple[int, int] | None = self.parse_contract_year_month(symbol, exchange)
        if contract_ym is None:
            return

        year, month = contract_ym
        start_year, start_month = self.shift_month(year, month, -months)
        self.start_date_edit.setDate(QtCore.QDate(start_year, start_month, 1))

    @staticmethod
    def shift_month(year: int, month: int, month_delta: int) -> tuple[int, int]:
        """
        Month arithmetic helper.
        月份偏移计算工具函数。
        """
        total_months: int = year * 12 + (month - 1) + month_delta
        new_year: int = total_months // 12
        new_month: int = total_months % 12 + 1
        return new_year, new_month

    def set_end_date_from_start(self, months: int) -> None:
        """
        Set end date by adding N months from current start date.
        以当前开始日期为基准增加N个月设置结束日期。
        """
        start_date: QtCore.QDate = self.start_date_edit.date()
        year: int = start_date.year()
        month: int = start_date.month()
        day: int = start_date.day()

        target_year, target_month = self.shift_month(year, month, months)
        target_date: QtCore.QDate = QtCore.QDate(target_year, target_month, day)

        # Fallback for month-end overflow (e.g. Jan 31 + 1 month).
        # 处理月底溢出日期（如1月31日加1个月）。
        if not target_date.isValid():
            month_start: QtCore.QDate = QtCore.QDate(target_year, target_month, 1)
            day_offset: int = day - 1
            target_date = month_start.addDays(day_offset)

        self.end_date_edit.setDate(target_date)

    @staticmethod
    def parse_contract_year_month(symbol: str, exchange: Exchange) -> tuple[int, int] | None:
        """
        Parse YYMM (or CZCE-specific format) from symbol.
        从代码解析YYMM（或郑商所特例格式）。
        """
        if exchange == Exchange.CZCE:
            return BatchDownloadDialog.parse_czce_year_month(symbol)

        match: re.Match[str] | None = re.search(r"(\d{2})(\d{2})$", symbol)
        if not match:
            return None

        year: int = 2000 + int(match.group(1))
        month: int = int(match.group(2))
        if month < 1 or month > 12:
            return None
        return year, month

    @staticmethod
    def parse_czce_year_month(symbol: str) -> tuple[int, int] | None:
        """
        Parse CZCE YMM format and infer decade near current year.
        解析郑商所YMM格式，并推断接近当前年的十位年份。
        """
        match: re.Match[str] | None = re.search(r"(\d)(\d{2})$", symbol)
        if not match:
            return None

        year_digit: int = int(match.group(1))
        month: int = int(match.group(2))
        if month < 1 or month > 12:
            return None

        current_year: int = datetime.now().year
        decade_start: int = current_year // 10 * 10
        year: int = decade_start + year_digit
        if year < current_year - 5:
            year += 10
        elif year > current_year + 4:
            year -= 10

        return year, month

    def add_task(self) -> None:
        """
        Validate form and append one in-memory batch task.
        校验输入参数并新增一条内存任务。
        """
        symbol: str = self.symbol_edit.text().strip()
        if not symbol:
            QtWidgets.QMessageBox.warning(self, "参数错误", "代码不能为空")
            return

        exchange: Exchange = self.exchange_combo.currentData()
        interval: Interval = self.interval_combo.currentData()
        dividend_type: str = self.dividend_combo.currentData()

        start_date = self.start_date_edit.date()
        start: datetime = datetime(start_date.year(), start_date.month(), start_date.day()).replace(tzinfo=DB_TZ)

        end_date = self.end_date_edit.date()
        end: datetime = datetime(end_date.year(), end_date.month(), end_date.day()) + timedelta(days=1)
        end = end.replace(tzinfo=DB_TZ)

        if end <= start:
            QtWidgets.QMessageBox.warning(self, "参数错误", "结束日期必须晚于开始日期")
            return

        # End date is stored as [start, end) convention, so +1 day above.
        # 结束日期使用左闭右开区间，因此前面做了+1天处理。
        self.engine.add_batch_download_task(
            symbol=symbol,
            exchange=exchange,
            interval=interval,
            dividend_type=dividend_type,
            start=start,
            end=end,
        )
        self.refresh_table()

    def start_batch_download(self) -> None:
        """
        Trigger engine-side asynchronous batch workflow.
        启动引擎侧异步批量下载流程。
        """
        success: bool
        message: str
        success, message = self.engine.start_batch_download()
        if success:
            QtWidgets.QMessageBox.information(self, "批量下载", message)
        else:
            QtWidgets.QMessageBox.warning(self, "批量下载", message)
        self.refresh_table()

    def remove_selected_tasks(self) -> None:
        """
        Remove selected rows if tasks are not running.
        删除选中行对应任务（运行中任务会被跳过）。
        """
        task_ids: list[int] = []
        for item in self.table.selectedItems():
            row: int = item.row()
            id_item: QtWidgets.QTableWidgetItem | None = self.table.item(row, 0)
            if not id_item:
                continue
            task_ids.append(int(id_item.text()))

        if not task_ids:
            return

        # Deduplicate because QTableWidget returns one item per selected cell.
        # 表格按单元格返回选中项，因此先按任务ID去重。
        unique_ids: list[int] = list(set(task_ids))
        removed_count, skipped_count = self.engine.remove_batch_download_tasks(unique_ids)
        QtWidgets.QMessageBox.information(
            self,
            "删除结果",
            f"已删除 {removed_count} 条，跳过 {skipped_count} 条运行中任务",
        )
        self.refresh_table()

    def clear_completed_tasks(self) -> None:
        """
        Clear completed tasks from in-memory list.
        清空内存中的已完成任务。
        """
        removed_count: int = self.engine.clear_completed_batch_download_tasks()
        QtWidgets.QMessageBox.information(self, "清理完成", f"已清理 {removed_count} 条任务")
        self.refresh_table()

    def refresh_table(self) -> None:
        """
        Pull task snapshots from engine and refresh table cells.
        从引擎拉取任务快照并刷新表格。
        """
        tasks: list[BatchDownloadTask] = self.engine.get_batch_download_tasks()

        self.table.setRowCount(len(tasks))
        for row, task in enumerate(tasks):
            self.table.setItem(row, 0, DataCell(str(task.task_id)))
            self.table.setItem(row, 1, DataCell(task.symbol))
            self.table.setItem(row, 2, DataCell(task.exchange.value))
            self.table.setItem(row, 3, DataCell(task.interval.value))
            self.table.setItem(row, 4, DataCell(task.dividend_type))
            self.table.setItem(row, 5, DataCell(task.start.strftime("%Y-%m-%d")))
            self.table.setItem(row, 6, DataCell((task.end - timedelta(days=1)).strftime("%Y-%m-%d")))
            self.table.setItem(row, 7, DataCell(task.status))
            self.table.setItem(row, 8, DataCell(task.error_message))

        # Disable start button while current batch is running.
        # 批量执行期间禁用“开始”按钮，避免重复启动。
        running: bool = self.engine.is_batch_download_running()
        self.start_button.setEnabled(not running)

    def closeEvent(self, event) -> None:  # type: ignore[override]
        """
        Stop timer before dialog closes.
        关闭对话框前停止定时刷新。
        """
        self.timer.stop()
        super().closeEvent(event)


class DownloadDialog(QtWidgets.QDialog):
    """"""

    def __init__(self, engine: ManagerEngine, parent: QtWidgets.QWidget | None = None) -> None:
        """"""
        super().__init__()

        self.engine: ManagerEngine = engine

        self.setWindowTitle("下载历史数据")
        self.setFixedWidth(300)

        self.symbol_edit: QtWidgets.QLineEdit = QtWidgets.QLineEdit()

        self.exchange_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        for i in Exchange:
            self.exchange_combo.addItem(str(i.name), i)

        self.interval_combo: QtWidgets.QComboBox = QtWidgets.QComboBox()
        for i in Interval:
            self.interval_combo.addItem(str(i.name), i)

        end_dt: datetime = datetime.now()
        start_dt: datetime = end_dt - timedelta(days=3 * 365)

        self.start_date_edit: QtWidgets.QDateEdit = QtWidgets.QDateEdit(
            QtCore.QDate(
                start_dt.year,
                start_dt.month,
                start_dt.day
            )
        )

        button: QtWidgets.QPushButton = QtWidgets.QPushButton("下载")
        button.clicked.connect(self.download)

        form: QtWidgets.QFormLayout = QtWidgets.QFormLayout()
        form.addRow("代码", self.symbol_edit)
        form.addRow("交易所", self.exchange_combo)
        form.addRow("周期", self.interval_combo)
        form.addRow("开始日期", self.start_date_edit)
        form.addRow(button)

        self.setLayout(form)

    def download(self) -> None:
        """"""
        symbol: str = self.symbol_edit.text()
        exchange: Exchange = Exchange(self.exchange_combo.currentData())
        interval: Interval = Interval(self.interval_combo.currentData())

        start_date = self.start_date_edit.date()
        start: datetime = datetime(start_date.year(), start_date.month(), start_date.day())
        start = start.replace(tzinfo=DB_TZ)

        if interval == Interval.TICK:
            count: int = self.engine.download_tick_data(symbol, exchange, start, self.output)
        else:
            count = self.engine.download_bar_data(symbol, exchange, interval, start, self.output)

        QtWidgets.QMessageBox.information(self, "下载结束", f"下载总数据量：{count}条")

    def output(self, msg: str) -> None:
        """输出下载过程中的日志"""
        QtWidgets.QMessageBox.warning(
            self,
            "数据下载",
            msg,
            QtWidgets.QMessageBox.Ok,
            QtWidgets.QMessageBox.Ok,
        )
