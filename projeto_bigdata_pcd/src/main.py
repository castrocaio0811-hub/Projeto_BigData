import os
import shutil
import subprocess
import pandas as pd

# ETAPA DE LIMPEZA DE DADOS


def executar_limpeza():
    print("🚿 Iniciando processo de limpeza de dados...")

    caminho_script = os.path.join(os.path.dirname(__file__), "limpeza", "limpar_dados.py")

    if os.path.exists(caminho_script):
        subprocess.run(["python", caminho_script], check=True)
        print("Limpeza concluída com sucesso.")
    else:
        print("Arquivo limpar_dados.py não encontrado!")


# MOVER ARQUIVO LIMPO PARA /data

def mover_dados_processados():
    # Origem - arquivo criado pelo limpar_dados.py
    origem = os.path.join(os.path.dirname(__file__), "limpeza", "dados_processados.csv")

    # Destino - pasta data no nível acima de src
    destino_pasta = os.path.join(os.path.dirname(__file__), "..", "data")
    destino_pasta = os.path.normpath(destino_pasta)
    destino = os.path.join(destino_pasta, "dados_processados.csv")

    print(f"Origem: {origem}")
    print(f"Destino: {destino}")
    print(f"Pasta destino existe? {os.path.exists(destino_pasta)}")

    #Garante que a pasta data existe
    if not os.path.exists(destino_pasta):
        print(f"Criando pasta: {destino_pasta}")
        os.makedirs(destino_pasta)

    #Verifica se a pasta foi criada
    if not os.path.exists(destino_pasta):
        print(f"Não foi possível criar a pasta: {destino_pasta}")
        return

    if os.path.exists(origem):
        print(f"Arquivo de origem: {origem}")
        try:
            shutil.copy2(origem, destino)
            print(f"Arquivo movido para: {destino}")
        except Exception as e:
            print(f"Erro ao copiar arquivo {e}")
    else:
        print(f"Arquivo de origem não encontrado: {origem}")

    #Lista o que existe na pasta limpeza para debug
    pasta_limpeza = os.path.join(os.path.dirname(__file__), "limpeza")
    if os.path.exists(pasta_limpeza):
        print(f"Conteúdo da pasta limpeza: {os.listdir(pasta_limpeza)}")

    if os.path.isfile(origem):
        os.remove(origem)
        print(f"Arquivo '{origem}' deletado com sucesso.")
    else:
        print(f"Erro o arquivo '{origem}' não foi encontrado.")



# 3️⃣ ETAPA DE ANÁLISE


"""def executar_analise():
    caminho_dados = os.path.join("data", "dados_processados.csv")
    if os.path.exists(caminho_dados):
        print("📊 Iniciando análise com dados limpos...")
        df = pd.read_csv(caminho_dados)
        analise_principal.gerar_graficos_principais(df)
        print("Análise concluída com sucesso.")
    else:
        print("Arquivo de dados limpos não encontrado em /data.")"""



# 4️⃣ MONITORAMENTO E AVALIAÇÃO (M&A)


def gerar_monitoramento(df):
    print("\n📈 Indicadores de Monitoramento e Avaliação (M&A)")

    indicadores = {
        "Percentual de dados coletados": f"{(len(df) / len(df)) * 100:.2f}%",
        "Número de registros tratados": len(df),
        "Número de dashboards gerados": 1,  # exemplo, pode ser dinâmico
        "Frequência de avaliação": "Mensal (processos) / Trimestral (impactos)"
    }

    for k, v in indicadores.items():
        print(f" - {k}: {v}")



# 5️⃣ EXECUÇÃO COMPLETA DO PIPELINE

if __name__ == "__main__":
    print("🚀 Iniciando pipeline Big Data PCD...\n")


    executar_limpeza()
    mover_dados_processados()

    caminho_dados_final = os.path.join("data", "dados_processados.csv")
    if os.path.exists(caminho_dados_final):
        df_final = pd.read_csv(caminho_dados_final)
        #executar_analise()
        gerar_monitoramento(df_final)

    print("\n🏁 Pipeline finalizado com sucesso!")
