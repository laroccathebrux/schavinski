import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
import os
import asyncio
import datetime
from aiohttp import ClientSession

# Criar diretórios necessários
def garantir_diretorios():
    os.makedirs('data', exist_ok=True)
    os.makedirs('logs', exist_ok=True)

garantir_diretorios()

# Chave da API OpenCageData (substitua pela sua)
OPENCAGE_API_KEY = "71b8c95ea704443d870f79edcc39d4d9"

# Função para registrar logs de erros
def registrar_erro(cep, motivo, resposta=None):
    with open("logs/erros.log", "a") as log_file:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_file.write(f"[{timestamp}] CEP: {cep} - Erro: {motivo}\n")
        if resposta:
            log_file.write(f"  Resposta da API: {resposta}\n")

# Função assíncrona para obter coordenadas e informações adicionais
async def obter_coordenadas(cep, session):
    url_brasilapi = f"https://brasilapi.com.br/api/cep/v2/{cep}"

    try:
        async with session.get(url_brasilapi) as response:
            if response.status == 429:
                await asyncio.sleep(3)
                return await obter_coordenadas(cep, session)
            
            data = await response.json()
            if response.status == 200 and "location" in data:
                lat = float(data["location"]["coordinates"]["latitude"])
                lon = float(data["location"]["coordinates"]["longitude"])
                state = data.get("state", "")
                city = data.get("city", "")
                neighborhood = data.get("neighborhood", "")
                street = data.get("street", "")
                service = data.get("service", "")
                return lat, lon, state, city, neighborhood, street, service
            else:
                registrar_erro(cep, "BrasilAPI não retornou dados completos", data)
    except Exception as e:
        registrar_erro(cep, f"Erro ao acessar BrasilAPI: {str(e)}")
    
    return None, None, "", "", "", "", ""

# Interface do Streamlit
st.title("\U0001F4CD Mapa de Bolhas - Vendas por CEP (Atualização em Tempo Real)")

uploaded_file = st.file_uploader("Carregue o arquivo CSV original", type=["csv"])

# Carregar arquivos existentes
existing_files = [f for f in os.listdir('data') if f.endswith('.csv')]
selected_existing_file = st.selectbox("Ou selecione um arquivo salvo:", ["Nenhum"] + existing_files)

data = None
filename = None

if selected_existing_file != "Nenhum":
    filename = f'data/{selected_existing_file}'
    data = pd.read_csv(filename, dtype={'cep': str})
    st.success(f"📂 Arquivo carregado: **{selected_existing_file}**")
elif uploaded_file:
    data = pd.read_csv(uploaded_file, sep=';', dtype=str)
    data.columns = data.columns.str.strip().str.lower().str.replace('"', '')
    data['cep'] = data['cep'].str.replace('"', '').str.replace('-', '').str.strip()
    data['quantidade'] = data['quantidade'].str.replace('"', '').str.replace(' ', '').str.replace(',', '.').astype(float)

    data = data.groupby('cep', as_index=False).agg({'quantidade': 'sum'})

    data['lat'] = None
    data['lon'] = None
    data['state'] = None
    data['city'] = None
    data['neighborhood'] = None
    data['street'] = None
    data['service'] = None

    filename = f"data/dados_{uploaded_file.name}"
else:
    st.warning("⚠️ Por favor, carregue um arquivo ou selecione um existente para continuar.")
    st.stop()

dados_pendentes = data[data['lat'].isnull() | data['lon'].isnull()]
total_pendentes = len(dados_pendentes)

barra_progresso = st.progress(0)
texto_progresso = st.empty()

async def processar_ceps():
    global data
    async with ClientSession() as session:
        tarefas = []
        indices = list(dados_pendentes.index)
        
        for idx in indices:
            tarefas.append(obter_coordenadas(data.at[idx, 'cep'], session))

        resultados = await asyncio.gather(*tarefas)

        for i, idx in enumerate(indices):
            data.at[idx, 'lat'], data.at[idx, 'lon'], data.at[idx, 'state'], data.at[idx, 'city'], data.at[idx, 'neighborhood'], data.at[idx, 'street'], data.at[idx, 'service'] = resultados[i]

            porcentagem = int(((i + 1) / total_pendentes) * 100)
            barra_progresso.progress(porcentagem / 100)
            texto_progresso.text(f"🔄 Obtendo coordenadas: {porcentagem}% concluído...")

        if total_pendentes > 0:
            data.to_csv(filename, index=False)
            st.success(f"✅ Coordenadas obtidas e salvas: `{filename}`")

if total_pendentes > 0:
    st.warning("🔄 Obtendo coordenadas... Isso pode levar alguns minutos.")
    asyncio.run(processar_ceps())

dados_mapa = data.dropna(subset=['lat', 'lon'])
mapa = folium.Map(location=[-14.2350, -51.9253], zoom_start=5)

for _, row in dados_mapa.iterrows():
    folium.CircleMarker(
        location=[row['lat'], row['lon']],
        radius=max(3, row['quantidade'] / dados_mapa['quantidade'].max() * 20),
        popup=f"📍 CEP: {row['cep']}<br>🛒 Quantidade: {row['quantidade']}<br>🏙️ Cidade: {row['city']}<br>🏡 Bairro: {row['neighborhood']}<br>📍 Rua: {row['street']}",
        color='blue',
        fill=True,
        fill_opacity=0.6
    ).add_to(mapa)

st.header("🌎 Mapa Interativo das Vendas (Atualizando em Tempo Real)")
folium_static(mapa)

if os.path.exists("logs/erros.log"):
    st.subheader("⚠️ Erros registrados:")
    with open("logs/erros.log", "r") as log_file:
        erros = log_file.readlines()
        if erros:
            st.text_area("CEP não encontrados e motivos:", value="".join(erros), height=150)
        else:
            st.success("✅ Nenhum erro registrado!")

st.subheader("📁 Baixar arquivo processado:")
if os.path.exists(filename):
    with open(filename, "rb") as file:
        st.download_button(
            label="⬇️ Baixar CSV Processado",
            data=file,
            file_name=os.path.basename(filename),
            mime="text/csv"
        )
