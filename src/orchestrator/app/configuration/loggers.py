from kitstructlog import InitLoggers, LoggerReg


class Loggers(InitLoggers):
    main = LoggerReg(name="MAIN", level=LoggerReg.Level.INFO)
    providers = LoggerReg(name="PROVIDERS", level=LoggerReg.Level.WARNING)
    handlers = LoggerReg(name="HANDLERS", level=LoggerReg.Level.INFO)
    engine = LoggerReg(name="ENGINE", level=LoggerReg.Level.INFO)
    nats = LoggerReg(name="NATS", level=LoggerReg.Level.INFO)
    oopsys = LoggerReg(name="OOPSYS", level=LoggerReg.Level.INFO)
    development = LoggerReg(name="DEVELOPMENT", level=LoggerReg.Level.DEBUG)

    def __init__(self, *, developer_mode: bool = False) -> None:
        if developer_mode:
            for attr_name in dir(self.__class__):
                attr = getattr(self.__class__, attr_name)
                if isinstance(attr, LoggerReg) and attr.level is not LoggerReg.Level.NONE:
                    attr.level = LoggerReg.Level.DEBUG

        super().__init__(developer_mode=developer_mode)
