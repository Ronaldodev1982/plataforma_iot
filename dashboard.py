import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import sqlalchemy
import plotly.express as px
from datetime import datetime


# Configuração da página do Streamlit
st.set_page_config(page_title="Dashboard de Análise de Dados", layout="wide")

# Função para carregar os dados do banco de dados
@st.cache_data
def load_data():
    db_url = "postgresql+psycopg2://postgres:2Smx'P?8[#RA\#9Z@192.168.210.40/IOTDatabase"
    engine = sqlalchemy.create_engine(db_url)
    
    try:
        connection = engine.connect()
        connection.close()
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None
    
    query = "SELECT * FROM tabela_tratada_sensor_rmc;"
    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Erro ao carregar os dados: {e}")
        return None
    
    return df

# Função para garantir que os dados são numéricos
def convert_to_numeric(df, exclude_columns):
    for col in df.columns:
        if col not in exclude_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

# Função para filtrar o DataFrame baseado em condições
def apply_filters(df, filters):
    for col, condition, value in filters:
        if condition == '==':
            df = df[df[col] == value]
        elif condition == '!=':
            df = df[df[col] != value]
        elif condition == '>=':
            df = df[df[col] >= value]
        elif condition == '<=':
            df = df[df[col] <= value]
        elif condition == '>':
            df = df[df[col] > value]
        elif condition == '<':
            df = df[df[col] < value]
    return df

# Carregar os dados
df = load_data()

# Se os dados foram carregados com sucesso, continuar com o dashboard
if df is not None:
    # Definir colunas a serem excluídas da conversão numérica
    exclude_columns = ['ID', 'Data', 'Hora']

    # Converter dados para tipos numéricos quando possível
    df = convert_to_numeric(df, exclude_columns)

    # Adicionar logotipo
    st.image("download (1).png", width=150)  # Substitua "eletromidia_logo.png" pelo caminho para a sua imagem

    # Título do Dashboard
    st.title("Dashboard de Análise de Dados dos Sensores RMC")

    # Exibir os primeiros dados para referência
    st.write("Primeiros dados da Tabela Tratada Sensor:")
    st.write(df.head())

    # Filtros adicionais
    st.sidebar.header("Filtros Adicionais")
    if 'Data' in df.columns:
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')
        start_date = st.sidebar.date_input("Data inicial", df['Data'].min())
        end_date = st.sidebar.date_input("Data final", df['Data'].max())
        df = df[(df['Data'] >= pd.Timestamp(start_date)) & (df['Data'] <= pd.Timestamp(end_date))]

    # Seleção de variáveis para análise
    st.sidebar.header("Seleção de Variáveis")
    variables = st.sidebar.multiselect("Selecione as variáveis para análise:", [col for col in df.columns if col not in exclude_columns])

    # Parâmetros de comparação
    st.sidebar.header("Definir Parâmetros de Comparação")
    filters = []
    if variables:
        for var in variables:
            condition = st.sidebar.selectbox(f"Condição para {var}", ['==', '!=', '>=', '<=', '>', '<'])
            value = st.sidebar.text_input(f"Valor para {var}", value="")
            if value:
                filters.append((var, condition, float(value)))  # Convertendo valor para float, ajuste conforme necessário

    # Aplicar filtros
    df = apply_filters(df, filters)

    # Seleção do tipo de gráfico
    st.sidebar.header("Seleção do Tipo de Gráfico")
    graph_type = st.sidebar.selectbox("Selecione o tipo de gráfico:", ["Pizza", "Pareto", "Barras", "Linhas", "Histograma", "Box Plot", "Scatter Plot"])

    # Limitar a quantidade de dados exibidos
    max_rows = st.sidebar.slider("Número máximo de linhas para exibir:", 1, len(df), 100)
    df = df.head(max_rows)

    # Definir paleta de cores personalizada
    # color_palette = px.colors.qualitative.Bold
    color_palette = ['#ff6361', '#58508d', '#ffa600', '#bc5090', '#003f5c', '#2f4b7c', '#665191', '#d45087']

    # Função para gerar gráfico de pizza
    def plot_pie(data):
        st.write("Gráfico de Pizza:")
        fig = px.pie(data, names=data.index, values=data.values, title="Gráfico de Pizza", 
                    color_discrete_sequence=color_palette, width=1000, height=700)
        st.plotly_chart(fig, use_container_width=True)

    # Função para gerar gráfico de Pareto
    def plot_pareto(data):
        st.write("Gráfico de Pareto:")
        data_counts = data.value_counts().sort_values(ascending=False)
        pareto_fig = px.bar(data_counts, x=data_counts.index, y=data_counts.values, title="Gráfico de Pareto", 
                            color_discrete_sequence=color_palette, width=1000, height=700)
        pareto_fig.update_layout(yaxis_title='Frequência')
        pareto_fig.update_layout(yaxis2=dict(
            title='Porcentagem Acumulada',
            overlaying='y',
            side='right',
            tickvals=data_counts.cumsum() / data_counts.sum() * 100,
            ticktext=[f'{x:.1f}%' for x in (data_counts.cumsum() / data_counts.sum() * 100)]
        ))
        st.plotly_chart(pareto_fig, use_container_width=True)

    # Função para gerar gráfico de barras
    def plot_bar(data):
        st.write("Gráfico de Barras:")
        bar_fig = px.bar(data, title="Gráfico de Barras", color_discrete_sequence=color_palette, width=1000, height=700)
        st.plotly_chart(bar_fig, use_container_width=True)

    # Função para gerar gráfico de linhas
    def plot_line(data):
        st.write("Gráfico de Linhas:")
        line_fig = px.line(data, title="Gráfico de Linhas", color_discrete_sequence=color_palette, width=1000, height=700)
        st.plotly_chart(line_fig, use_container_width=True)

    # Função para gerar histograma
    def plot_histogram(data):
        st.write("Histograma:")
        hist_fig = px.histogram(data, title="Histograma", color_discrete_sequence=color_palette, width=1000, height=700)
        st.plotly_chart(hist_fig, use_container_width=True)

    # Função para gerar box plot
    def plot_box(data):
        st.write("Box Plot:")
        box_fig = px.box(data, title="Box Plot", color_discrete_sequence=color_palette, width=1000, height=700)
        st.plotly_chart(box_fig, use_container_width=True)

    # Função para gerar scatter plot
    def plot_scatter(data):
        if len(data.columns) >= 2:
            st.write("Scatter Plot:")
            scatter_fig = px.scatter(data, x=data.columns[0], y=data.columns[1], title="Scatter Plot", 
                                    color_discrete_sequence=color_palette, width=1000, height=700)
            st.plotly_chart(scatter_fig, use_container_width=True)
        else:
            st.write("Por favor, selecione pelo menos duas variáveis para o scatter plot.")



    # Exportar dados
    st.sidebar.header("Exportar Dados")
    if st.sidebar.button("Exportar CSV"):
        csv = df.to_csv(index=False)
        st.sidebar.download_button(label="Baixar CSV", data=csv, file_name='dados_exportados.csv', mime='text/csv')
    # Filtrar o DataFrame com as variáveis selecionadas
    if variables:
        df_filtered = df[variables]
        
        # Garantir que os dados estão no tipo correto para cada tipo de gráfico
        if graph_type == "Pizza":
            if len(variables) == 1:
                plot_pie(df[variables[0]].value_counts())
            else:
                st.write("Por favor, selecione exatamente uma variável para o gráfico de pizza.")
        elif graph_type == "Pareto":
            if len(variables) == 1:
                plot_pareto(df[variables[0]])
            else:
                st.write("Por favor, selecione exatamente uma variável para o gráfico de Pareto.")
        elif graph_type == "Barras":
            if df_filtered.select_dtypes(include='number').empty:
                st.write("Por favor, selecione variáveis numéricas para o gráfico de barras.")
            else:
                plot_bar(df_filtered)
        elif graph_type == "Linhas":
            if df_filtered.select_dtypes(include='number').empty:
                st.write("Por favor, selecione variáveis numéricas para o gráfico de linhas.")
            else:
                plot_line(df_filtered)
        elif graph_type == "Histograma":
            if df_filtered.select_dtypes(include='number').empty:
                st.write("Por favor, selecione variáveis numéricas para o histograma.")
            else:
                plot_histogram(df_filtered)
        elif graph_type == "Box Plot":
            if df_filtered.select_dtypes(include='number').empty:
                st.write("Por favor, selecione variáveis numéricas para o box plot.")
            else:
                plot_box(df_filtered)
        elif graph_type == "Scatter Plot":
            if df_filtered.select_dtypes(include='number').shape[1] >= 2:
                plot_scatter(df_filtered)
            else:
                st.write("Por favor, selecione pelo menos duas variáveis numéricas para o scatter plot.")
    else:
        st.write("Por favor, selecione pelo menos uma variável para análise.")