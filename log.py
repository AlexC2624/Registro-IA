import logging
import os
from logging.handlers import RotatingFileHandler

class LOG:
    def __init__(self, log_file_path='data/log_db.log', max_bytes=1024 * 1024, backup_count=3):
        logger = logging.getLogger(os.path.basename(__file__))
        logger.setLevel(logging.DEBUG)

        # Cria um RotatingFileHandler
        # maxBytes: o tamanho máximo do arquivo de log antes que ele seja rotacionado (aqui, 25 MB).
        # backupCount: o número de arquivos de log de backup a serem mantidos (aqui, 5).
        #              Quando um novo arquivo é criado, se o número de backups exceder backupCount,
        #              o arquivo mais antigo é removido.
        rotating_handler = RotatingFileHandler(
            filename=log_file_path,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8' # Boa prática para evitar problemas de codificação
        )
        rotating_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        rotating_handler.setFormatter(formatter)

        if not logger.handlers: # Adiciona o handler apenas se ainda não houver nenhum
            logger.addHandler(rotating_handler)
        
        self.logger = logger

    def create_log(self, comando_sql, strg_fixa='SQL Executado: '):
        if 'SELECT' in comando_sql or 'PRAGMA' in comando_sql or 'BEGIN' in comando_sql or 'COMMIT' in comando_sql:
            self.logger.debug(strg_fixa + comando_sql)

        elif 'INSERT' in comando_sql or 'CREATE' in comando_sql or 'UPDATE' in comando_sql:
            self.logger.info(strg_fixa + comando_sql)

        elif 'DELETE' in comando_sql or 'DROP' in comando_sql:
            self.logger.warning(strg_fixa + comando_sql)
        
        else: self.logger.error(strg_fixa + comando_sql)

# Exemplo de uso:
# Dentro da sua classe ou script onde você configura o logger
# logger_db = create_log(log_file_path='data/log_db.log', max_bytes=5 * 1024 * 1024, backup_count=3)
# conn.set_trace_callback(lambda comando_sql: logger_db.debug(f'SQL Executado: {comando_sql}'))