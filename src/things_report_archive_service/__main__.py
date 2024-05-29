import config

from things_report_archive_service import service

log = config.get_logger()


def main() -> None:
    log.info("Starting service")

    things_report_archive_service = service.ThingsReportArchiveService()
    things_report_archive_service.poll()


if __name__ == "__main__":
    main()
