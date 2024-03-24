from processing import run
from settings import Settings

if __name__ == '__main__':
    settings = Settings(
        server="DESKTOP-9F8A8PF\\MSSQLSERVER01",
        database="Datawarehouse",
        data_dir="data/",
        log_dir="logs/"
    )

    run(settings)