def load_environ() -> None:
    import os
    from dotenv import load_dotenv

    if os.path.exists(os.path.join(os.path.dirname(__file__), '.env')):
        load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
