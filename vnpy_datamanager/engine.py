import csv
from datetime import datetime
from queue import Empty, Queue
from threading import Lock, Thread
from dataclasses import dataclass, replace
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from collections.abc import Callable

from vnpy.trader.engine import BaseEngine, MainEngine, EventEngine
from vnpy.trader.logger import logger
from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.object import BarData, TickData, ContractData, HistoryRequest
from vnpy.trader.database import BaseDatabase, get_database, BarOverview, DB_TZ
from vnpy.trader.datafeed import BaseDatafeed, get_datafeed
from vnpy.trader.utility import ZoneInfo
from vnpy.trader.setting import SETTINGS

APP_NAME = "DataManager"

TASK_STATUS_PENDING: str = "待下载"
TASK_STATUS_DOWNLOADING: str = "下载中"
TASK_STATUS_PROCESSING: str = "处理中"
TASK_STATUS_FINISHED: str = "已完成"
TASK_STATUS_FAILED: str = "失败"


@dataclass
class BatchDownloadTask:
    """Batch task model stored in memory only. / 仅存内存的批量任务模型。"""

    task_id: int
    symbol: str
    exchange: Exchange
    interval: Interval
    dividend_type: str
    start: datetime
    end: datetime
    status: str = TASK_STATUS_PENDING
    created_at: datetime | None = None
    finished_at: datetime | None = None
    error_message: str = ""
    failed_stage: str = ""
    attempt_count: int = 0
    data_count: int = 0


class ManagerEngine(BaseEngine):
    """"""

    def __init__(
        self,
        main_engine: MainEngine,
        event_engine: EventEngine,
    ) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.database: BaseDatabase = get_database()
        self.datafeed: BaseDatafeed = get_datafeed()
        self.data_processing_workers: int = max(1, int(SETTINGS.get("data_processing_workers", 2)))

        self.batch_tasks: dict[int, BatchDownloadTask] = {}
        self.batch_task_ids: list[int] = []
        self.batch_lock: Lock = Lock()
        self.batch_task_index: int = 0
        self.download_queue: Queue[int] = Queue()

        self.batch_running: bool = False
        self.batch_download_thread: Thread | None = None
        self.process_pool: ThreadPoolExecutor | None = None

    def import_data_from_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        tz_name: str,
        datetime_head: str,
        open_head: str,
        high_head: str,
        low_head: str,
        close_head: str,
        volume_head: str,
        turnover_head: str,
        open_interest_head: str,
        datetime_format: str
    ) -> tuple:
        """"""
        with open(file_path) as f:
            buf: list = [line.replace("\0", "") for line in f]

        reader: csv.DictReader = csv.DictReader(buf, delimiter=",")

        bars: list[BarData] = []
        start: datetime | None = None
        count: int = 0
        tz: ZoneInfo = ZoneInfo(tz_name)

        for item in reader:
            if datetime_format:
                dt: datetime = datetime.strptime(item[datetime_head], datetime_format)
            else:
                dt = datetime.fromisoformat(item[datetime_head])
            dt = dt.replace(tzinfo=tz)

            turnover = item.get(turnover_head, 0)
            open_interest = item.get(open_interest_head, 0)

            bar: BarData = BarData(
                symbol=symbol,
                exchange=exchange,
                datetime=dt,
                interval=interval,
                volume=float(item[volume_head]),
                open_price=float(item[open_head]),
                high_price=float(item[high_head]),
                low_price=float(item[low_head]),
                close_price=float(item[close_head]),
                turnover=float(turnover),
                open_interest=float(open_interest),
                gateway_name="DB",
            )

            bars.append(bar)

            # do some statistics
            count += 1
            if not start:
                start = bar.datetime

        end: datetime = bar.datetime

        # insert into database
        self.database.save_bar_data(bars)

        return start, end, count

    def output_data_to_csv(
        self,
        file_path: str,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> bool:
        """"""
        bars: list[BarData] = self.load_bar_data(symbol, exchange, interval, start, end)

        fieldnames: list = [
            "symbol",
            "exchange",
            "datetime",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "turnover",
            "open_interest"
        ]

        try:
            with open(file_path, "w") as f:
                writer: csv.DictWriter = csv.DictWriter(f, fieldnames=fieldnames, lineterminator="\n")
                writer.writeheader()

                for bar in bars:
                    d: dict = {
                        "symbol": bar.symbol,
                        "exchange": bar.exchange.value,
                        "datetime": bar.datetime.strftime("%Y-%m-%d %H:%M:%S"),
                        "open": bar.open_price,
                        "high": bar.high_price,
                        "low": bar.low_price,
                        "close": bar.close_price,
                        "turnover": bar.turnover,
                        "volume": bar.volume,
                        "open_interest": bar.open_interest,
                    }
                    writer.writerow(d)

            return True
        except PermissionError:
            return False

    def get_bar_overview(self) -> list[BarOverview]:
        """"""
        overview: list[BarOverview] = self.database.get_bar_overview()
        return overview

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> list[BarData]:
        """"""
        bars: list[BarData] = self.database.load_bar_data(
            symbol,
            exchange,
            interval,
            start,
            end
        )

        return bars

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """"""
        count: int = self.database.delete_bar_data(
            symbol,
            exchange,
            interval
        )

        return count

    def download_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        output: Callable,
        end: datetime | None = None,
        dividend_type: str = "none",
    ) -> int:
        """
        Query bar data from datafeed.
        """
        data: list[BarData] = self.query_bar_data(
            symbol,
            exchange,
            interval,
            start,
            output,
            end=end,
            dividend_type=dividend_type,
        )

        if data:
            self.database.save_bar_data(data)
            return (len(data))

        logger.bind(gateway_name="DataManager").warning(
            "下载bar数据返回0条，"
            f"symbol={symbol}，exchange={exchange.value}，"
            f"start={start}，end={end}，"
            f"query_tick_history_response_type={type(data).__name__}，"
            f"query_tick_history_response={data!r}"
        )
        return 0

    def query_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        output: Callable,
        end: datetime | None = None,
        dividend_type: str = "none",
    ) -> list[BarData]:
        """
        Query bar data without saving into DB.
        查询K线数据但不入库。
        """
        if end is None:
            end = datetime.now(DB_TZ)

        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            interval=Interval(interval),
            start=start,
            end=end
        )
        req.dividend_type = dividend_type

        vt_symbol: str = f"{symbol}.{exchange.value}"
        contract: ContractData | None = self.main_engine.get_contract(vt_symbol)

        if contract and contract.history_data:
            data: list[BarData] = self.main_engine.query_history(req, contract.gateway_name)
        else:
            data = self.datafeed.query_bar_history(req, output)

        return data or []

    def download_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        output: Callable,
        end: datetime | None = None,
        dividend_type: str = "none",
    ) -> int:
        """
        Query tick data from datafeed.
        """
        data: list[TickData] = self.query_tick_data(
            symbol,
            exchange,
            start,
            output,
            end=end,
            dividend_type=dividend_type
        )

        if data:
            self.database.save_tick_data(data)
            return (len(data))

        logger.bind(gateway_name="DataManager").warning(
            "下载Tick数据返回0条，"
            f"symbol={symbol}，exchange={exchange.value}，"
            f"start={start}，end={end}，"
            f"query_tick_history_response_type={type(data).__name__}，"
            f"query_tick_history_response={data!r}"
        )
        return 0

    def query_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        output: Callable,
        end: datetime | None = None,
        dividend_type: str = "none",
    ) -> list[TickData]:
        """
        Query tick data without saving into DB.
        查询Tick数据但不入库。
        """
        if end is None:
            end = datetime.now(DB_TZ)

        req: HistoryRequest = HistoryRequest(
            symbol=symbol,
            exchange=exchange,
            start=start,
            end=end
        )
        req.dividend_type = dividend_type

        data: list[TickData] = self.datafeed.query_tick_history(req, output)
        return data or []

    def add_batch_download_task(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        dividend_type: str,
        start: datetime,
        end: datetime,
    ) -> BatchDownloadTask:
        """
        Create one in-memory task and return a snapshot.
        创建一条内存任务并返回快照。
        """
        with self.batch_lock:
            self.batch_task_index += 1
            task_id: int = self.batch_task_index
            task: BatchDownloadTask = BatchDownloadTask(
                task_id=task_id,
                symbol=symbol,
                exchange=exchange,
                interval=interval,
                dividend_type=dividend_type,
                start=start,
                end=end,
                created_at=datetime.now(DB_TZ),
            )
            self.batch_tasks[task_id] = task
            self.batch_task_ids.append(task_id)
            return replace(task)

    def get_batch_download_tasks(self) -> list[BatchDownloadTask]:
        """
        Get snapshot list for UI rendering.
        获取任务快照列表，供UI刷新使用。
        """
        with self.batch_lock:
            return [replace(self.batch_tasks[task_id]) for task_id in self.batch_task_ids]

    def remove_batch_download_tasks(self, task_ids: list[int]) -> tuple[int, int]:
        """
        Remove selected tasks (non-running only).
        删除选中任务（仅允许删除非运行中任务）。
        Return: (removed_count, skipped_count) / 返回：(删除数量, 跳过数量)
        """
        removed: int = 0
        skipped: int = 0

        with self.batch_lock:
            for task_id in task_ids:
                task: BatchDownloadTask | None = self.batch_tasks.get(task_id)
                if task is None:
                    continue

                if task.status in (TASK_STATUS_DOWNLOADING, TASK_STATUS_PROCESSING):
                    skipped += 1
                    continue

                del self.batch_tasks[task_id]
                self.batch_task_ids.remove(task_id)
                removed += 1

        return removed, skipped

    def clear_completed_batch_download_tasks(self) -> int:
        """
        Remove all finished tasks from memory.
        清理内存中的全部已完成任务。
        """
        removed: int = 0
        with self.batch_lock:
            task_ids: list[int] = list(self.batch_task_ids)
            for task_id in task_ids:
                task: BatchDownloadTask = self.batch_tasks[task_id]
                if task.status == TASK_STATUS_FINISHED:
                    del self.batch_tasks[task_id]
                    self.batch_task_ids.remove(task_id)
                    removed += 1
        return removed

    def is_batch_download_running(self) -> bool:
        """Return current batch running flag. / 返回批量下载运行状态。"""
        return self.batch_running

    def start_batch_download(self, output: Callable | None = None) -> tuple[bool, str]:
        """
        Start async workflow: single downloader + processing pool.
        启动异步流程：单下载线程 + 处理线程池。
        """
        if self.batch_running:
            return False, "批量下载正在进行中"

        with self.batch_lock:
            pending_task_ids: list[int] = [
                task_id
                for task_id in self.batch_task_ids
                if self.batch_tasks[task_id].status == TASK_STATUS_PENDING
            ]

        if not pending_task_ids:
            return False, "没有待下载任务"

        # Build a fresh queue for this run.
        # 为本轮运行创建全新的下载队列。
        self.download_queue = Queue()
        for task_id in pending_task_ids:
            self.download_queue.put(task_id)

        self.batch_running = True
        # Download is serialized by one thread; processing/save is parallelized by pool.
        # 下载严格单线程串行；处理/入库交给线程池并行。
        self.process_pool = ThreadPoolExecutor(max_workers=self.data_processing_workers)
        self.batch_download_thread = Thread(
            target=self._run_batch_download,
            args=(pending_task_ids, output),
            daemon=True,
            name="DataManagerBatchDownload",
        )
        self.batch_download_thread.start()
        return True, f"开始批量下载，任务总数：{len(pending_task_ids)}"

    def _run_batch_download(self, run_task_ids: list[int], output: Callable | None = None) -> None:
        """
        Download loop (single-threaded), then wait all processing futures.
        单线程下载循环，并等待全部处理任务完成。
        """
        futures: list[Future] = []

        try:
            while True:
                try:
                    task_id: int = self.download_queue.get_nowait()
                except Empty:
                    break

                with self.batch_lock:
                    task: BatchDownloadTask | None = self.batch_tasks.get(task_id)
                    if task is None or task.status != TASK_STATUS_PENDING:
                        continue
                    # State: pending -> downloading
                    # 状态流转：待下载 -> 下载中
                    task.status = TASK_STATUS_DOWNLOADING
                    task.attempt_count += 1
                    task.error_message = ""
                    task.failed_stage = ""

                try:
                    data = self._query_task_data(task, output)
                except Exception as ex:
                    self._mark_task_failed(task_id, "download", str(ex), output)
                    continue

                if not data:
                    self._mark_task_failed(task_id, "download", "下载返回空数据", output)
                    continue

                with self.batch_lock:
                    current_task: BatchDownloadTask | None = self.batch_tasks.get(task_id)
                    if not current_task:
                        continue
                    # State: downloading -> processing
                    # 状态流转：下载中 -> 处理中
                    current_task.status = TASK_STATUS_PROCESSING

                if self.process_pool:
                    future: Future = self.process_pool.submit(self._process_task_data, task_id, data, output)
                    futures.append(future)

            # Ensure all processing results are collected before finishing this batch run.
            # 结束前等待所有处理线程完成，确保状态与日志完整。
            for future in as_completed(futures):
                future.result()
        finally:
            if self.process_pool:
                self.process_pool.shutdown(wait=True)
                self.process_pool = None
            self.batch_running = False
            self._log_batch_summary(run_task_ids, output)

    def _query_task_data(
        self,
        task: BatchDownloadTask,
        output: Callable | None = None,
    ) -> list[BarData] | list[TickData]:
        """
        Query data by interval type for one task.
        按任务周期类型查询对应历史数据。
        """
        if task.interval == Interval.TICK:
            return self.query_tick_data(
                task.symbol,
                task.exchange,
                task.start,
                output or print,
                end=task.end,
                dividend_type=task.dividend_type,
            )

        return self.query_bar_data(
            task.symbol,
            task.exchange,
            task.interval,
            task.start,
            output or print,
            end=task.end,
            dividend_type=task.dividend_type,
        )

    def _process_task_data(
        self,
        task_id: int,
        data: list[BarData] | list[TickData],
        output: Callable | None = None,
    ) -> None:
        """
        Save downloaded data and mark task completion.
        将下载数据入库并更新任务完成状态。
        """
        try:
            if isinstance(data[0], TickData):
                self.database.save_tick_data(data)
            else:
                self.database.save_bar_data(data)
        except Exception as ex:
            self._mark_task_failed(task_id, "save", str(ex), output)
            return

        with self.batch_lock:
            task: BatchDownloadTask | None = self.batch_tasks.get(task_id)
            if not task:
                return
            # State: processing -> finished
            # 状态流转：处理中 -> 已完成
            task.status = TASK_STATUS_FINISHED
            task.finished_at = datetime.now(DB_TZ)
            task.data_count = len(data)

            success_msg: str = (
                f"{task.symbol} {task.exchange.value} {task.interval.value} "
                f"下载并入库完成，数量：{task.data_count}"
            )
        self._write_batch_log(success_msg, output)

    def _mark_task_failed(
        self,
        task_id: int,
        stage: str,
        error_message: str,
        output: Callable | None = None,
    ) -> None:
        """
        Mark task failed and persist failure details.
        标记任务失败并记录失败阶段与错误信息。
        """
        with self.batch_lock:
            task: BatchDownloadTask | None = self.batch_tasks.get(task_id)
            if not task:
                return
            task.status = TASK_STATUS_FAILED
            task.finished_at = datetime.now(DB_TZ)
            task.failed_stage = stage
            task.error_message = error_message

            failed_msg: str = (
                f"{task.symbol} {task.exchange.value} {task.interval.value} "
                f"失败[{stage}]：{error_message}"
            )
        self._write_batch_log(failed_msg, output)

    def _log_batch_summary(self, run_task_ids: list[int], output: Callable | None = None) -> None:
        """
        Print final summary for this run only.
        输出本轮任务的最终汇总日志。
        """
        with self.batch_lock:
            tasks: list[BatchDownloadTask] = [
                self.batch_tasks[task_id]
                for task_id in run_task_ids
                if task_id in self.batch_tasks
            ]
            success_count: int = sum(task.status == TASK_STATUS_FINISHED for task in tasks)
            failed_count: int = sum(task.status == TASK_STATUS_FAILED for task in tasks)
            pending_count: int = sum(task.status == TASK_STATUS_PENDING for task in tasks)

        self._write_batch_log(
            f"批量下载结束：成功 {success_count}，失败 {failed_count}，待下载 {pending_count}",
            output,
        )

    def _write_batch_log(self, msg: str, output: Callable | None = None) -> None:
        """
        Unified logging sink for batch workflow.
        批量流程统一日志出口。
        """
        self.main_engine.write_log(msg, APP_NAME)
        if output:
            output(msg)
