#!/usr/bin/env python3
"""
MAIN FINAL - Versão que resolve problemas de importação
"""

import os
import sys
import importlib.util
import logging

# Adicionar o diretório atual explicitamente ao sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

print(f"📁 Diretório do script: {SCRIPT_DIR}")
print(f"📦 Python path: {sys.path}")

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def import_module_from_file(module_name, file_path):
    """Importa um módulo diretamente do arquivo."""
    try:
        spec = importlib.util.spec_from_file_location(module_name, file_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        logger.info(f"✅ Módulo '{module_name}' importado de '{file_path}'")
        return module
    except Exception as e:
        logger.error(f"❌ Falha ao importar '{module_name}' de '{file_path}': {e}")
        return None

def main():
    """Função principal."""
    logger.info("🚀 Iniciando pipeline ETL")
    
    # Verificar se os arquivos existem
    modules_to_load = {
        'database_handler': os.path.join(SCRIPT_DIR, 'database_handler.py'),
        'etl_anp': os.path.join(SCRIPT_DIR, 'etl_anp.py'),
        'etl_dim_tempo': os.path.join(SCRIPT_DIR, 'etl_dim_tempo.py'),
        'etl_macro': os.path.join(SCRIPT_DIR, 'etl_macro.py'),
    }
    
    loaded_modules = {}
    
    for name, path in modules_to_load.items():
        if os.path.exists(path):
            module = import_module_from_file(name, path)
            if module:
                loaded_modules[name] = module
        else:
            logger.error(f"❌ Arquivo não encontrado: {path}")
    
    if len(loaded_modules) != len(modules_to_load):
        logger.error("❌ Nem todos os módulos puderam ser carregados")
        return False
    
    # Agora podemos usar os módulos
    try:
        logger.info("📊 Inicializando DatabaseHandler...")
        db_handler = loaded_modules['database_handler'].DatabaseHandler()
        logger.info("✅ DatabaseHandler criado com sucesso")
        
        # Aqui você continuaria com o resto da lógica...
        logger.info("🎉 Todos os módulos foram carregados com sucesso!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro durante a execução: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)