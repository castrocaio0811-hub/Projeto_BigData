from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import glob
import shutil
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.express as px
import unicodedata


# ==========================
# 🔹 Inicializar Spark
# ==========================
spark = SparkSession.builder.appName("AcessibilidadeEscolar").getOrCreate()

# ==========================
# 🔹 Arquivos de saída
# ==========================
arquivo_unido_csv = "escolas_pcd_todos_anos.csv"
arquivo_unido_excel = "escolas_pcd_todos_anos.xlsx"


# ==========================
# 🔹 Função remover caracteres bugados e padronizar
# ==========================
def limpar_unicode(x):
    if isinstance(x, str):
        # NFKD quebra acentos e evita erro ao converter
        x = unicodedata.normalize("NFKD", x)
        # converte para latin1 removendo chars que não existem lá
        x = x.encode("latin1", "ignore").decode("latin1")
    return x


# ==========================
# 🔹 Converter CSV UTF-8 → Latin1 sem caracteres bugados
# ==========================
def converter_para_latin1(arquivo_utf8, arquivo_saida):
    df = pd.read_csv(arquivo_utf8, sep=';', encoding='utf-8')

    # Limpa todas as colunas de texto
    df = df.apply(lambda col: col.map(limpar_unicode) if col.dtype == object else col)

    df.to_csv(arquivo_saida, sep=';', index=False, encoding='latin1')


# ==========================
# 🔹 Função para unir os anos
# ==========================
def unir_anos():
    anos = [2022, 2023, 2024]
    dfs = []

    colunas_interesse = [
        "NU_ANO_CENSO",
        "NO_REGIAO",
        "SG_UF",
        "NO_MUNICIPIO",
        "TP_DEPENDENCIA",
        "IN_ACESSIBILIDADE_RAMPAS",
        "IN_ACESSIBILIDADE_ELEVADOR",
        "IN_BANHEIRO_PNE",
        "IN_RESERVA_PCD",
        "QT_MAT_BAS",
        "QT_MAT_FUND",
        "QT_MAT_MED",
        "QT_DOC_BAS",
        "QT_DOC_FUND",
        "QT_DOC_MED"
    ]

    for ano in anos:
        caminho = f"microdados_ed_basica_{ano}.csv"
        if not os.path.exists(caminho):
            print(f"⚠️ Arquivo {caminho} não encontrado. Pulando...")
            continue

        df = spark.read.csv(caminho, header=True, sep=';', inferSchema=True)

        df_reduzido = df.select([c for c in colunas_interesse if c in df.columns])
        df_reduzido = df_reduzido.withColumn("NU_ANO_CENSO", F.lit(ano))

        df_reduzido = (
            df_reduzido
            .withColumnRenamed("IN_ACESSIBILIDADE_RAMPAS", "acess_rampa")
            .withColumnRenamed("IN_ACESSIBILIDADE_ELEVADOR", "acess_elevador")
            .withColumnRenamed("IN_BANHEIRO_PNE", "acess_banheiro_pcd")
            .withColumnRenamed("IN_RESERVA_PCD", "politica_pcd")
            .withColumnRenamed("QT_MAT_BAS", "matriculas_basico")
            .withColumnRenamed("QT_MAT_FUND", "matriculas_fundamental")
            .withColumnRenamed("QT_MAT_MED", "matriculas_medio")
            .withColumnRenamed("QT_DOC_BAS", "docentes_basico")
            .withColumnRenamed("QT_DOC_FUND", "docentes_fundamental")
            .withColumnRenamed("QT_DOC_MED", "docentes_medio")
        )

        dfs.append(df_reduzido)

    if not dfs:
        raise ValueError("Nenhum arquivo válido encontrado para unir.")

    df_todos_anos = dfs[0]
    for df in dfs[1:]:
        df_todos_anos = df_todos_anos.unionByName(df, allowMissingColumns=True)

    df_filtrado = df_todos_anos.filter(df_todos_anos["politica_pcd"] == 1)

    # -----------------------------
    # 🔥 SALVA CSV TEMPORÁRIO UTF-8
    # -----------------------------
    df_filtrado.coalesce(1).write.option("header", True).option("sep", ";") \
        .option("encoding", "utf-8").mode("overwrite").csv("saida_pcd_todos_anos_temp")

    pasta_temp = "saida_pcd_todos_anos_temp"
    arquivo_temp = glob.glob(os.path.join(pasta_temp, "*.csv"))[0]

    arquivo_utf8 = "escolas_utf8_temp.csv"
    shutil.move(arquivo_temp, arquivo_utf8)
    shutil.rmtree(pasta_temp)

    # ------------------------------------------------
    # 🔥 CONVERTE PARA LATIN1 SEM BUGAR ACENTOS
    # ------------------------------------------------
    converter_para_latin1(arquivo_utf8, arquivo_unido_csv)

    # ------------------------------------------------
    # 🔥 GERAR EXCEL TAMBÉM LIMPO
    # ------------------------------------------------
    df_pd = pd.read_csv(arquivo_unido_csv, sep=';', encoding='latin1')
    df_pd.to_excel(arquivo_unido_excel, index=False)

    print("🎉 CSV (latin1) e Excel gerados com sucesso!")
    return df_pd


# ==========================
# 🔹 Carregar ou unir os dados
# ==========================
if os.path.exists(arquivo_unido_csv) and os.path.exists(arquivo_unido_excel):
    print("✔️ Arquivos unidos já existem. Carregando CSV...")
    df_pd = pd.read_csv(arquivo_unido_csv, sep=';', encoding="latin1")
else:
    df_pd = unir_anos()


# ==========================
# 🔹 Pré-processamento
# ==========================
df_pd["indice_acessibilidade"] = (
    df_pd["acess_rampa"]
    + df_pd["acess_elevador"]
    + df_pd["acess_banheiro_pcd"]
)

df_pd["ratio_docente_basico"] = df_pd["matriculas_basico"] / df_pd["docentes_basico"]
df_pd["ratio_docente_fund"] = df_pd["matriculas_fundamental"] / df_pd["docentes_fundamental"]
df_pd["ratio_docente_medio"] = df_pd["matriculas_medio"] / df_pd["docentes_medio"]

anos_disponiveis = sorted(df_pd["NU_ANO_CENSO"].unique())
regioes_disponiveis = sorted(df_pd["NO_REGIAO"].unique())


# ==========================
# 🔹 Criar app Dash
# ==========================
app = dash.Dash(__name__)
app.title = "Dashboard PCDs Educação"

app.layout = html.Div([
    html.H1("Acessibilidade escolar para PCDs", style={'text-align': 'center'}),

    html.Div([
        html.Label("Ano:"),
        dcc.Dropdown(id='dropdown-ano',
                     options=[{'label': str(a), 'value': a} for a in anos_disponiveis],
                     value=anos_disponiveis[0]),

        html.Label("Região:"),
        dcc.Dropdown(id='dropdown-regiao',
                     options=[{'label': r, 'value': r} for r in regioes_disponiveis] + [{'label': 'Todas', 'value': 'Todas'}],
                     value='Todas'),
    ], style={'width': '40%', 'margin': '0 auto', 'text-align': 'center'}),

    html.Br(),
    html.Div(id='graficos-dash')
], style={'min-height': '100vh', 'padding': '20px'})


# ==========================
# 🔹 Callback
# ==========================
@app.callback(
    Output('graficos-dash', 'children'),
    Input('dropdown-ano', 'value'),
    Input('dropdown-regiao', 'value')
)
def atualizar_graficos(ano, regiao):

    df_filtrado_dash = df_pd[df_pd['NU_ANO_CENSO'] == ano]
    if regiao != 'Todas':
        df_filtrado_dash = df_filtrado_dash[df_filtrado_dash['NO_REGIAO'] == regiao]

    graficos = []

    # Matrículas
    for col, titulo in [('matriculas_basico', 'Básico'),
                        ('matriculas_fundamental', 'Fundamental'),
                        ('matriculas_medio', 'Médio')]:
        df_top = df_filtrado_dash.groupby("SG_UF")[col].sum().reset_index()
        df_top = df_top.sort_values(col, ascending=False).head(10)
        graficos.append(dcc.Graph(
            figure=px.bar(df_top, x="SG_UF", y=col,
                          title=f"Top 10 UFs por Matrículas {titulo} (Maiores)")
        ))

        df_piores = df_filtrado_dash.groupby("SG_UF")[col].sum().reset_index()
        df_piores = df_piores.sort_values(col, ascending=True).head(10)
        graficos.append(dcc.Graph(
            figure=px.bar(df_piores, x="SG_UF", y=col,
                          title=f"Top 10 UFs por Matrículas {titulo} (Menores)")
        ))

    # Índice médio de acessibilidade
    df_acess = df_filtrado_dash.groupby("SG_UF")["indice_acessibilidade"].mean().reset_index()

    graficos.append(dcc.Graph(
        figure=px.bar(df_acess.sort_values('indice_acessibilidade', ascending=False).head(10),
                      x="SG_UF", y="indice_acessibilidade",
                      title="Top 10 UFs com os melhores Índice de Acessibilidade")
    ))

    graficos.append(dcc.Graph(
        figure=px.bar(df_acess.sort_values('indice_acessibilidade', ascending=True).head(10),
                      x="SG_UF", y="indice_acessibilidade",
                      title="Top 10 UFs com os piores Índice de Acessibilidade")
    ))

    # Índice por município
    df_cidades = (
        df_filtrado_dash
        .groupby(["NO_MUNICIPIO", "SG_UF"])['indice_acessibilidade']
        .mean()
        .reset_index()
    )

    df_cidades["cidade_uf"] = df_cidades["NO_MUNICIPIO"] + " (" + df_cidades["SG_UF"] + ")"

    graficos.append(dcc.Graph(
        figure=px.bar(df_cidades.sort_values('indice_acessibilidade', ascending=False).head(50),
                      x="cidade_uf", y="indice_acessibilidade",
                      title="Top 50 Cidades com Maior Índice de Acessibilidade")
    ))

    graficos.append(dcc.Graph(
        figure=px.bar(df_cidades.sort_values('indice_acessibilidade', ascending=True).head(50),
                      x="cidade_uf", y="indice_acessibilidade",
                      title="Top 50 Cidades com Menor Índice de Acessibilidade")
    ))

    return graficos


# ==========================
# 🔹 Rodar app
# ==========================
if __name__ == "__main__":
    app.run(debug=True, port=8050)
