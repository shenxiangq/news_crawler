[loggers]
keys=root

[handlers]
keys=fileHandler

[formatters]
keys=fileFormatter

[logger_root]
level=DEBUG
handlers=fileHandler

[handler_fileHandler]
class=handlers.RotatingFileHandler
level=DEBUG
formatter=fileFormatter
args=('../log/seed_mining.log', 'a', 50*1024*1024, 5)

[formatter_fileFormatter]
format=%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s
datefmt=%a, %d %b %Y %H:%M:%S
