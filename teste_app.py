# Imports padrão do Python
from datetime import datetime

# Imports do Streamlit
import streamlit as st
from streamlit_option_menu import option_menu
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

# Imports do Pandas
import pandas as pd  # Garante que o Pandas seja importado globalmente

# Imports do PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, sum, format_number
from pyspark.sql.types import DoubleType


# ==== CONFIGURAÇÃO DA PÁGINA ====
st.set_page_config(page_title="Validação DRE", page_icon="💱", layout="wide")
st.title("PLANEJAMENTO ORÇAMENTÁRIO")

# ==== CARREGAMENTO DOS DADOS ====
df_hierarquia = pd.read_parquet("C:/pyspark/Orcamento/App/Arquivos/Parquet/hierarquiaDRE")
df_valores = pd.read_csv("C:/pyspark/Orcamento/App/Arquivos/valores.csv", sep=";")

# Tratamento colunas
df_valores['Data'] = pd.to_datetime(df_valores['Data']).dt.normalize()
if df_valores['Saldo'].dtype != float:
    df_valores['Saldo'] = df_valores['Saldo'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)

# ==== SESSÃO ESTADO ====
if "df_valores" not in st.session_state:
    st.session_state.df_valores = df_valores.copy()

if "df_valores_pendentes" not in st.session_state:
    st.session_state.df_valores_pendentes = pd.DataFrame(columns=st.session_state.df_valores.columns)

# ==== SIDEBAR: LOGO + MENU + CONTADOR ====
with st.sidebar:
    st.image("C:/pyspark/Orcamento/App/Imagens/Stec.png", use_container_width=True)

    menu = option_menu(
        menu_title="Menu",
        options=["Validar DRE" , "Editar Lançamentos" ],
        icons=["file-earmark-text", "table"],
        menu_icon= "cast",
        default_index=0,
        orientation="vertical"
    )

    pendentes_count = len(st.session_state.df_valores_pendentes)
    st.markdown(f"###### 🔄 Lançamentos pendentes para salvar: {pendentes_count}")

# ==== FUNÇÕES ====

def salvar_csv_e_atualizar():
    """Salva o CSV e limpa o cache para atualizar o DRE"""
    df_salvar = pd.concat([st.session_state.df_valores, st.session_state.df_valores_pendentes], ignore_index=True)
    df_salvar.to_csv("C:/pyspark/Orcamento/App/Arquivos/valores.csv", sep=";", index=False)
    st.session_state.df_valores = df_salvar.copy()
    st.session_state.df_valores_pendentes = pd.DataFrame(columns=st.session_state.df_valores.columns)
    
    # Limpar cache para forçar recarregamento dos dados
    carregar_dados_valores.clear()
    
    st.success("Lançamentos salvos com sucesso! DRE será atualizado automaticamente.")
    st.rerun()  # Reinicia o app para refletir as mudanças

def lancar_valor():
    with st.expander("➕ Novo Lançamento"):
        st.subheader("Lançar Informações")

        data = st.date_input("Data", value=datetime.today())
        centro_custo = st.selectbox(
            "Centro Custo",
            ["Selecione"] + sorted(df_hierarquia["cd_centro_custo_dre"].unique())
        )
        conta_contabil = st.selectbox(
            "Conta Contábil",
            ["Selecione"] + sorted(df_hierarquia["cd_conta_contabil_dre"].unique())
        )

        # Filtra hierarquias conforme centro de custo e conta contábil selecionados
        if centro_custo != "Selecione" and conta_contabil != "Selecione":
            hierarquias_filtradas = df_hierarquia[
                (df_hierarquia["cd_centro_custo_dre"] == centro_custo) &
                (df_hierarquia["cd_conta_contabil_dre"] == conta_contabil)
            ]["sk_hierarquia_dre"].unique()
            hierarquia_options = ["Selecione"] + sorted(hierarquias_filtradas)
        else:
            hierarquia_options = ["Selecione"]

        hierarquia = st.selectbox("Hierarquia DRE", hierarquia_options)

        saldo = st.number_input("Saldo", step=100.0)

        if st.button("Lançar"):
            if centro_custo == "Selecione" or conta_contabil == "Selecione" or hierarquia == "Selecione":
                st.error("Por favor, selecione todas as opções para lançar o valor.")
                return

            desc_conta = df_hierarquia.loc[
                (df_hierarquia["cd_conta_contabil_dre"] == conta_contabil) &
                (df_hierarquia["sk_hierarquia_dre"] == hierarquia), "ds_grupo_nivel_dre"
            ]
            desc_conta_val = desc_conta.values[0] if not desc_conta.empty else ""

            nova_linha = {
                "Data": pd.to_datetime(data).normalize(),
                "sk_hierarquia_dre": hierarquia,
                "Cod. Conta Contábil PN": conta_contabil,
                "Desc. Conta Contábil PN": desc_conta_val,
                "Cod. Centro Custo": centro_custo,
                "Saldo": saldo
            }

            st.session_state.df_valores_pendentes = pd.concat(
                [st.session_state.df_valores_pendentes, pd.DataFrame([nova_linha])],
                ignore_index=True
            )
            st.success("Valor lançado e pendente para salvar.")

    # --- TABELA UNIFICADA COM VISUALIZAÇÃO E EXCLUSÃO ---
    st.markdown("### Tabela de Valores")


    df_total = pd.concat([st.session_state.df_valores, st.session_state.df_valores_pendentes], ignore_index=True)
    df_total['id'] = df_total.index.astype(int)

   
    gb = GridOptionsBuilder.from_dataframe(df_total)
    gb.configure_selection(selection_mode='multiple', use_checkbox=True)
    grid_options = gb.build()




    grid_response = AgGrid(
        df_total,
        gridOptions=grid_options,
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        allow_unsafe_jscode=True,
        height=400,
        fit_columns_on_grid_load=True,
    )

    selected_rows = grid_response.get('selected_rows')
    if selected_rows is None:
        selected_rows = []

    # Se selected_rows for um DataFrame (pode acontecer), converte para lista de dicts
    if hasattr(selected_rows, "to_dict"):
        selected_rows = selected_rows.to_dict('records')

    if len(selected_rows) > 0:
        st.warning(f"{len(selected_rows)} lançamentos selecionados para exclusão.")

        if st.button("🗑️ Excluir lançamentos selecionados"):
            # Detecta automaticamente o tipo de dado
            if isinstance(selected_rows[0], dict):
                selected_ids = [int(row["id"]) for row in selected_rows]
            else:
                selected_ids = [int(row) for row in selected_rows]

            # Filtra e atualiza os dataframes
            df_filtrado = df_total[~df_total["id"].isin(selected_ids)].drop(columns=["id"])

            len_df_valores = len(st.session_state.df_valores)
            st.session_state.df_valores = df_filtrado.iloc[:len_df_valores].copy()
            st.session_state.df_valores_pendentes = df_filtrado.iloc[len_df_valores:].copy()

            # Salvar alterações no CSV e atualizar DRE
            st.session_state.df_valores.to_csv("C:/pyspark/Orcamento/App/Arquivos/valores.csv", sep=";", index=False)
            carregar_dados_valores.clear()  # Limpar cache
            
            st.success("Lançamentos excluídos e arquivo atualizado! DRE será atualizado automaticamente.")
            st.rerun()

    # Opções para salvar ou descartar pendentes
    if len(st.session_state.df_valores_pendentes) > 0:
        col1, col2 = st.columns(2)
        with col1:
            if st.button("💾 Salvar lançamentos pendentes no CSV"):
                salvar_csv_e_atualizar()  # Função atualizada
        with col2:
            if st.button("🗑️ Descartar lançamentos pendentes"):
                st.session_state.df_valores_pendentes = pd.DataFrame(columns=st.session_state.df_valores.columns)
                st.warning("Lançamentos pendentes descartados.")
                st.rerun()

@st.cache_data
def carregar_dados_hierarquia():
    """Carrega dados da hierarquia DRE"""
    try:
        path_hierarquia = 'C:/pyspark/Orcamento/Output/gold/Parametro_DRE/10-Premium Distribuidora/dimHierarquiaDRE/dados.parquet'
        df_hierarquia = pd.read_parquet(path_hierarquia)
        return df_hierarquia
    except Exception as e:
        # Usar o df_hierarquia já carregado no início do script
        return df_hierarquia

@st.cache_data
def carregar_dados_valores():
    """Carrega e processa dados de valores"""
    try:
        # Carregar CSV atualizado
        df_valores = pd.read_csv(
            'C:/pyspark/Orcamento/App/Arquivos/valores.csv',
            sep=";",
            encoding='utf-8'
        )
        
        # Renomear coluna
        df_valores = df_valores.rename(columns={"Cod. Conta Contábil PN": "cd_conta_contabil_dre"})
        
        # Garantir que Saldo é float (valores já estão no formato correto)
        df_valores['Saldo'] = pd.to_numeric(df_valores['Saldo'], errors='coerce')
        
        # Agrupar valores
        df_valores_unico = df_valores.groupby(['cd_conta_contabil_dre', 'sk_hierarquia_dre'])['Saldo'].sum().reset_index()
        df_valores_unico = df_valores_unico.rename(columns={'Saldo': 'saldo'})
        
        return df_valores_unico
        
    except Exception as e:
        st.error(f"Erro ao carregar valores: {e}")
        return pd.DataFrame()

def processar_dre(df_hierarquia, df_valores, niveis_dre):
    """Processa os dados da DRE"""
    try:
        # Join dos dados
        df_joined = pd.merge(
            df_hierarquia,
            df_valores,
            on=['cd_conta_contabil_dre', 'sk_hierarquia_dre'],
            how='left'
        )
        
        # Filtrar dados
        df_filtrado = df_joined[
            (df_joined['id_nivel_dre'].isin(niveis_dre)) &
            (df_joined['sg_grupo_principal_dre'] == 'DRG')
        ].copy()
        
        # Agrupar e somar
        resultado = df_filtrado.groupby([
            'ds_grupo_nivel_dre', 
            'id_ordem_prm', 
            'id_nivel_dre'
        ])['saldo'].sum().reset_index()
        
        # Renomear coluna
        resultado = resultado.rename(columns={'saldo': 'valor_total'})
        
        # Ordenar por ordem
        resultado = resultado.sort_values('id_ordem_prm').reset_index(drop=True)
        
        # Tratar valores nulos
        resultado['valor_total'] = resultado['valor_total'].fillna(0)
        
        return resultado
        
    except Exception as e:
        st.error(f"Erro ao processar DRE: {e}")
        return pd.DataFrame()

def formatar_valor_brasileiro(valor):
    """Formata valor no padrão brasileiro"""
    return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def dre():
    st.markdown(" ## Validação de DRE")

    # Carregar dados (sempre atualizados devido ao cache)
    with st.spinner("Carregando dados..."):
        df_hierarquia_dre = carregar_dados_hierarquia()
        df_valores_dre = carregar_dados_valores()

    if df_hierarquia_dre.empty or df_valores_dre.empty:
        st.error("Não foi possível carregar os dados necessários.")
        return

    # Interface do usuário
    st.markdown(
    "<hr style='border:2px solid #B8D4C3; margin-top:10px; margin-bottom:10px;'>",
    unsafe_allow_html=True

    )

#    st.markdown("###### ⚙️ Configurações")
    col1 = st.columns(1)


 #   with col2:
 #       # Botão para forçar atualização manual
 #       if st.button("🔄 Atualizar DRE"):
 #           carregar_dados_valores.clear()
 #           st.rerun()

    # --- NOVO: Botão para incluir novo valor ---
    with col1[0]:
        if "show_expander_dre" not in st.session_state:
            st.session_state.show_expander_dre = False
        if st.button("➕ Novo Lançamento"):
            st.session_state.show_expander_dre = not st.session_state.show_expander_dre

    # --- NOVO: Expander de lançamento acima da tabela DRE ---
    if st.session_state.get("show_expander_dre", False):
        with st.expander("Formulário de Lançamento", expanded=True):
            st.subheader("Lançar Informações")

            data = st.date_input("Data", value=datetime.today(), key="dre_data")
            centro_custo = st.selectbox(
                "Centro Custo",
                ["Selecione"] + sorted(df_hierarquia_dre["cd_centro_custo_dre"].unique()),
                key="dre_centro_custo"
            )
            conta_contabil = st.selectbox(
                "Conta Contábil",
                ["Selecione"] + sorted(df_hierarquia_dre["cd_conta_contabil_dre"].unique()),
                key="dre_conta_contabil"
            )

            # Filtra hierarquias conforme centro de custo e conta contábil selecionados
            if centro_custo != "Selecione" and conta_contabil != "Selecione":
                hierarquias_filtradas = df_hierarquia_dre[
                    (df_hierarquia_dre["cd_centro_custo_dre"] == centro_custo) &
                    (df_hierarquia_dre["cd_conta_contabil_dre"] == conta_contabil)
                ]["sk_hierarquia_dre"].unique()
                hierarquia_options = ["Selecione"] + sorted(hierarquias_filtradas)
            else:
                hierarquia_options = ["Selecione"]

            hierarquia = st.selectbox("Hierarquia DRE", hierarquia_options, key="dre_hierarquia")

            saldo = st.number_input("Saldo", step=100.0, key="dre_saldo")

            if st.button("Lançar", key="dre_lancar"):
                if centro_custo == "Selecione" or conta_contabil == "Selecione" or hierarquia == "Selecione":
                    st.error("Por favor, selecione todas as opções para lançar o valor.")
                    return

                desc_conta = df_hierarquia_dre.loc[
                    (df_hierarquia_dre["cd_conta_contabil_dre"] == conta_contabil) &
                    (df_hierarquia_dre["sk_hierarquia_dre"] == hierarquia), "ds_grupo_nivel_dre"
                ]
                desc_conta_val = desc_conta.values[0] if not desc_conta.empty else ""

                nova_linha = {
                    "Data": pd.to_datetime(data).normalize(),
                    "sk_hierarquia_dre": hierarquia,
                    "Cod. Conta Contábil PN": conta_contabil,
                    "Desc. Conta Contábil PN": desc_conta_val,
                    "Cod. Centro Custo": centro_custo,
                    "Saldo": saldo
                }

                if "df_valores_pendentes" not in st.session_state:
                    st.session_state.df_valores_pendentes = pd.DataFrame(columns=st.session_state.df_valores.columns)
                st.session_state.df_valores_pendentes = pd.concat(
                    [st.session_state.df_valores_pendentes, pd.DataFrame([nova_linha])],
                    ignore_index=True
                )
                salvar_csv_e_atualizar()
                st.success("Lançamentos salvos com sucesso!")

                




    # Interface do usuário
    st.markdown(
    "<hr style='border:2px solid #B8D4C3; margin-top:10px; margin-bottom:10px;'>",
    unsafe_allow_html=True
)
    if "maior_nivel" not in st.session_state:
        st.session_state.maior_nivel = 1  # valor inicial padrão

    st.markdown("**Nível DRE:**")
    opcoes_dre = [1, 2, 3, 4]

    st.session_state.maior_nivel = st.radio(
        "",
        opcoes_dre,
        index=opcoes_dre.index(st.session_state.maior_nivel),
        horizontal=True
    )

    niveis_dre = list(range(1, st.session_state.maior_nivel + 1))



    # Processar dados
    with st.spinner("Processando DRE..."):
        df_resultado = processar_dre(df_hierarquia_dre, df_valores_dre, niveis_dre)

    if df_resultado.empty:
        st.warning("⚠️ Nenhum dado encontrado para os níveis selecionados.")
        return

    # Preparar dados para exibição
    df_display = df_resultado.copy()
    df_display['Valor Formatado'] = df_display['valor_total'].apply(formatar_valor_brasileiro)

    # Exibir tabela
    st.dataframe(
        df_display[['ds_grupo_nivel_dre', 'Valor Formatado']].rename(columns={
            'ds_grupo_nivel_dre': 'Grupo DRE',
            'Valor Formatado': 'Valor Total'
        }),
        use_container_width=True,
        hide_index=True,
        height=700  # altura em pixels (pode ajustar)
)
    


# ==== EXECUÇÃO ====

if menu == "Editar Lançamentos":
    lancar_valor()
else:
    dre()

