from dotenv import load_dotenv

from pipeline import Pipeline


def main():
    """
    Main function to run the pipeline.

    - Loads environment variables from a .env file.
    - Sets the directory for reports.
    - Initializes and runs the pipeline.
    """
    load_dotenv()
    reports_dir = 'reports'
    pipeline = Pipeline(reports_dir)
    pipeline.run()


if __name__ == '__main__':
    main()
