import pandas as pd
import datetime
import time
import os
import json

# Caminhos dos arquivos
arquivo_entrada = os.path.join(os.path.dirname(__file__), 'dados.json')
arquivo_saida = os.path.join(os.path.dirname(__file__), 'dados_processados.csv')
log_arquivo = os.path.join(os.path.dirname(__file__), 'log_importacao.txt')

# Tamanho dos blocos (quantas linhas ler por vez)
chunksize = 50000


def log(mensagem):
    """Registra mensagem no terminal e no arquivo de log"""
    hora = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    linha = f"{hora} — {mensagem}"
    print(linha)
    with open(log_arquivo, 'a', encoding='utf-8') as f:
        f.write(linha + '\n')


def importar_json():
    """Lê um arquivo JSON e converte para CSV"""
    inicio = time.time()
    try:
        log(f"Iniciando leitura de '{arquivo_entrada}'...")

        # Lê o arquivo JSON
        with open(arquivo_entrada, 'r', encoding='latin-1') as f:
            data = json.load(f)

        # Verifica se existe o campo 'body'
        if 'body' not in data:
            log("❌ O arquivo JSON não contém o campo 'body'. Nada foi convertido.")
            return

        # Converte o conteúdo de 'body' em DataFrame
        df = pd.DataFrame(data['body'])

        # Salva como CSV
        df.to_csv(arquivo_saida, index=False, encoding='latin-1')

        duracao = time.time() - inicio
        log(f"✅ Conversão concluída com sucesso! Arquivo salvo como '{arquivo_saida}'.")
        log(f"Tempo total: {duracao:.2f}s.")

    except Exception as e:
        log(f"❌ Erro durante a importação: {e}")


if __name__ == "__main__":
    importar_json()