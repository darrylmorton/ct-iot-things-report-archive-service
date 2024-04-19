import logging

from things_report_archive_service.service import ThingsReportArchiveService

log = logging.getLogger("things_report_archive_service")


def main() -> None:
    log.info("Starting service")

    service = ThingsReportArchiveService()
    service.poll()


if __name__ == "__main__":
    main()
