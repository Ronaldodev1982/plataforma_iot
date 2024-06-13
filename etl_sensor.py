import pandas as pd
import sqlalchemy
import streamlit as st
import schedule
import time

# Definir a URL de conexão para o SQLAlchemy
db_url = "postgresql+psycopg2://postgres:2Smx'P?8[#RA\#9Z@192.168.210.40/IOTDatabase"

# Criar o engine do SQLAlchemy
engine = sqlalchemy.create_engine(db_url)

#def etl_process():
    
# Definir a consulta SQL
query = "SELECT * FROM parseddata_x_artifact;"

# Carregar dados do banco de dados para um DataFrame
df = pd.read_sql(query, engine)

# Supondo que a coluna 'datastring' contenha os dados principais com o delimitador '|'
df_split = df['datastring'].str.split('|', expand=True)

# Para armazenar todos os DataFrames processados
result_dfs = []

# Processar cada coluna no DataFrame dividido
for col in df_split.columns:
    # Dividir cada célula em chave e valor usando '#'
    temp_df = df_split[col].str.split('#', expand=True)
    
    # Se houver apenas uma coluna após a divisão, renomeie-a para evitar erros
    if temp_df.shape[1] == 2:
        temp_df.columns = [f'{col}_chave', f'{col}_valor']
    else:
        temp_df.columns = [f'{col}_valor']
    
    #print(temp_df)
    
    # Dividir os valores onde há mais de uma informação usando ';'
    if f'{col}_valor' in temp_df.columns:
        # Garantir que não há valores nulos antes da divisão
        temp_df = temp_df.dropna(subset=[f'{col}_valor'])
        
        # Dividir os valores na coluna específica por ';'
        temp_df[f'{col}_valor'] = temp_df[f'{col}_valor'].str.split(';')
        
        # Contar o número de valores em cada célula para criar colunas adicionais
        max_length = temp_df[f'{col}_valor'].apply(len).max()
        
        # Expandir os valores em colunas adicionais
        temp_df = pd.concat([temp_df.drop(f'{col}_valor', axis=1),
                             temp_df[f'{col}_valor'].apply(pd.Series)], axis=1)
        
        # Renomear as novas colunas numericamente
        temp_df.columns = [f'{col}_chave' if x == f'{col}_chave' else f'{col}_valor_{i}'
                           for i, x in enumerate(temp_df.columns)]
        
        # Explodir as linhas onde há múltiplos valores
        #temp_df = temp_df.explode(f'{col}_valor')
    
    # Garantir que os índices são únicos
    temp_df = temp_df.reset_index(drop=True)

    result_dfs.append(temp_df)

# Concatenar todos os DataFrames processados
df_processed = pd.concat(result_dfs, axis=1)

# Concatenar o DataFrame original com o DataFrame processado
df_final = pd.concat([df.reset_index(drop=True), df_processed], axis=1)

# Remover colunas que não são mais necessárias
colunas_para_remover = ['datastring', 'datahorarmc', 'datahora', 'screen_data', '2_valor_5', '3_valor_4', '4_valor_25', '5_valor_7', '6_valor_4', '7_valor_3', '8_valor_7', '9_valor_2', '11_valor_0']
df_final.drop(columns=colunas_para_remover, inplace=True)

# Renomear colunas específicas (opcional)
df_final.rename(columns={
    'artifact' : 'ID',
    '0_chave': 'Porta',
    '0_valor_1': 'Status Porta',
    '1_chave': 'Agua',
    '1_valor_1': 'Status Agua',
    '2_chave' : 'Luminosidade',
    '2_valor_1' : 'Sensor de Brilho 1',
    '2_valor_2' : 'Backlight 1',
    '2_valor_3' : 'Sensor de Brilho 2',
    '2_valor_4' : 'Backlight 2',
    '3_chave' : 'Temperaturas de Entrada e Saída',
    '3_valor_1' : 'Entrada',
    '3_valor_2' : 'Saída 1',
    '3_valor_3' : 'Saída 2',
    '4_chave' : 'Ventilação Externa',
    '4_valor_1' : 'Ventilação 1',
    '4_valor_2' : 'Ventilação 2',
    '4_valor_3' : 'Ventilação 3',
    '4_valor_4' : 'Ventilação 4',
    '4_valor_5' : 'Ventilação 5',
    '4_valor_6' : 'Ventilação 6',
    '4_valor_7' : 'Ventilação 7',
    '4_valor_8' : 'Ventilação 8',
    '4_valor_9' : 'Ventilação 9',
    '4_valor_10' : 'Ventilação 10',
    '4_valor_11' : 'Ventilação 11',
    '4_valor_12' : 'Ventilação 12',
    '4_valor_13' : 'Ventilação 13',
    '4_valor_14' : 'Ventilação 14',
    '4_valor_15' : 'Ventilação 15',
    '4_valor_16' : 'Ventilação 16',
    '4_valor_17' : 'Ventilação 17',
    '4_valor_18' : 'Ventilação 18',
    '4_valor_19' : 'Ventilação 19',
    '4_valor_20' : 'Ventilação 20',
    '4_valor_21' : 'Ventilação 21',
    '4_valor_22' : 'Ventilação 22',
    '4_valor_23' : 'Ventilação 23',
    '4_valor_24' : 'Ventilação 24',
    '5_chave' : 'Métricas Elétricas',
    '5_valor_1' : 'Tensao_V',
    '5_valor_2' : 'Corrente_A',
    '5_valor_3' : 'Potencia Real_W',
    '5_valor_4' : 'Frequencia_Hz',
    '5_valor_5' : 'Consumo_W',
    '5_valor_6' : 'Potencia Reativa_KVAr',
    '6_chave' : 'Tensão de Saída',
    '6_valor_1' : 'Tensão de Saída 1',
    '6_valor_2' : 'Tensão de Saída 2',
    '6_valor_3' : 'Tensão de Saída 3',
    '7_chave' : 'Temperatura e Umidade Interna',
    '7_valor_1' : 'Temperatura Interna',
    '7_valor_2' : 'Umidade Interna',
    '8_chave' : 'G-Sensor',
    '8_valor_1' : 'G-Sensor_1',
    '8_valor_2' : 'G-Sensor_2',
    '8_valor_3' : 'G-Sensor_3',
    '8_valor_4' : 'G-Sensor_4',
    '8_valor_5' : 'G-Sensor_5',
    '8_valor_6' : 'G-Sensor_6',
    '9_chave' : 'G-Sensor_Tilt',
    '9_valor_1' : 'Dados do G-Sensor Tilt',
    '10_chave' : 'Data Hora Rmc',
    '10_valor_1' : 'Data Hora',
    '11_valor_1' : 'HDMI 1',
    '11_valor_2' : 'HDMI 2',
    '11_valor_3' : 'Brilho Tela 1',
    '11_valor_4' : 'Brilho Tela 2',
    '11_valor_5' : 'Sensor Brilho 1',
    '11_valor_6' : 'Sensor Brilho 2'
    
       

    # Adicione outras colunas conforme necessário
}, inplace=True)

df_final['Data Hora'] = pd.to_datetime(df_final['Data Hora'], format='%d/%m/%Y %H:%M:%S')

# Criar a coluna 'Data' no formato americano 'yyyy-MM-dd'
df_final['Data'] = df_final['Data Hora'].dt.strftime('%Y-%m-%d')

# Criar a coluna 'Hora' no formato 'HH:mm:ss'
df_final['Hora'] = df_final['Data Hora'].dt.strftime('%H:%M:%S')

# Opcional: Remover a coluna original 'Data Hora'
df_final.drop(columns=['Data Hora'], inplace=True)

# Especificar os tipos de dados como VARCHAR
df_final = df_final.astype(object)

# Inserir o DataFrame tratado em uma nova tabela no banco de dados
df_final.to_sql('tabela_tratada_sensor_rmc', engine, if_exists='replace', index=False)

print("Dados inseridos na tabela 'tabela_tratada_sensor_rmc' com sucesso.")


# Exibir o DataFrame resultante
st.write(df_final)