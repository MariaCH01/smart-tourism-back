class DevelopmentConfig():
    DEBUG = True
    MYSQL_HOST = 'geoportal.cjci4g2mo8ri.us-east-1.rds.amazonaws.com'
    MYSQL_USER = 'admin'
    MYSQL_PASSWORD = 'xGeo-Port43'
    MYSQL_DB = 'geoportal'
    
config = {
    'development': DevelopmentConfig
}