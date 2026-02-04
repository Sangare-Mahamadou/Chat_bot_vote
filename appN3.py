import streamlit as st
import plotly.express as px
from agent_sql_opti_Copie2 import ElectionAgent
import os, base64, logging

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIG STREAMLIT ---
st.set_page_config(
    page_title="Assistant Électoral CI 2025",
    layout="centered",
    page_icon="🗳️",
    initial_sidebar_state="expanded"
)

# --- FONCTIONS UTILES ---
def load_css(css_path: str, bg_image_path: str) -> None:
    """Charge le CSS et l'image de fond (base64)."""
    if os.path.exists(css_path):
        with open(css_path, "r", encoding="utf-8") as f:
            css = f.read()
        if os.path.exists(bg_image_path):
            with open(bg_image_path, "rb") as img:
                b64 = base64.b64encode(img.read()).decode()
            css = css.replace("var(--bg-image)", f'url("data:image/png;base64,{b64}")')
        st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)

def display_ambiguity_options():
    st.markdown('<div class="bot-bubble">🤔 Plusieurs correspondances trouvées. Veuillez préciser :</div>', unsafe_allow_html=True)
    st.columns(1) # Une seule colonne pour plus de lisibilité avec les noms longs
    
    for i, opt in enumerate(st.session_state.options):
        # On nettoie l'option pour n'avoir que la valeur (ex: "DIVO, COMMUNE")
        valeur_propre = opt.split(": ")[-1].strip()
        
        if st.button(opt, key=f"btn_ambig_{i}", width='content'):
            question_initiale = st.session_state.get('last_query', "")
            terme_ambigu = st.session_state.get('ambiguous_term', "")

            # Substitution de la question pour l'historique et pour l'agent
            if terme_ambigu and terme_ambigu.lower() in question_initiale.lower():
                st.session_state.context = valeur_propre
                st.session_state.options = None

                if st.session_state.history:
                    st.session_state.history[-1]["content"] = f"Résultats pour {valeur_propre}"

                st.rerun()
            else:
                nouvelle_question = f"Résultats pour {valeur_propre}"

            # --- ACTION CRITIQUE ---
            st.session_state.context = valeur_propre
            st.session_state.options = None  # On ferme les boutons
            
            # On remplace le dernier message utilisateur par la question corrigée
            if st.session_state.history and st.session_state.history[-1]["role"] == "user":
                st.session_state.history[-1]["content"] = nouvelle_question
                # ON EFFACE aussi les réponses précédentes (bot) si elles existent
                while len(st.session_state.history) > 1 and st.session_state.history[-2]["role"] == "assistant":
                    st.session_state.history.pop(-2)
            
            # On force le script à recharger pour entrer directement dans la phase SQL
            st.rerun()

# --- INITIALISATION AGENT ---
@st.cache_resource
def get_agent():
    try:
        return ElectionAgent("data/database/election_ci.db", "schema_for_agent.json")
    except Exception as e:
        st.error(f"Erreur initialisation base : {e}")
        st.stop()

agent = get_agent()

# --- SESSION STATE ---
if 'history' not in st.session_state: st.session_state.history = []
if 'context' not in st.session_state: st.session_state.context = None
if 'options' not in st.session_state: st.session_state.options = None
if 'count' not in st.session_state: st.session_state.count = 0

# --- CHARGEMENT CSS + IMAGE ---
load_css("style.css", "img/chat_bot2.png")

# --- SIDEBAR ---
with st.sidebar:
    st.title("🗳️ Élections 2025")
    st.metric("Requêtes effectuées", st.session_state.count)
    if st.button("🗑️ Réinitialiser le chat"):
        st.session_state.history = []
        st.session_state.context = None
        st.session_state.options = None
        st.rerun()
    stats = agent.schema.get("database_info", {}).get("statistics", {})
    with st.expander("📊 Données Clés"):
        st.write(f"• **Sièges :** {stats.get('total_elus', 205)}")
        st.write(f"• **Partis :** {stats.get('total_partis', 41)}")
        st.write(f"• **Candidats :** {stats.get('total_candidatures', 1125)}")

# --- INTERFACE CHAT ---
st.markdown('<h1 class="main-title">🗳️ Assistant Électoral 2025</h1>', unsafe_allow_html=True)

# Message d'accueil
if not st.session_state.history:
    st.markdown("""
    <div class="welcome-box">
        <h3>Analyse des Résultats (ARCI25)</h3>
        <p><small>Posez vos questions sur les statistiques pour plus de clarté..</small></p>
    </div>
    """, unsafe_allow_html=True)

# Affichage de l'historique
for chat in st.session_state.history:
    div_class = "user-bubble" if chat["role"] == "user" else "bot-bubble"
    st.markdown(f'<div class="{div_class}">{chat["content"]}</div>', unsafe_allow_html=True)
    if chat["role"] == "assistant":
        if "plotly_fig" in chat and chat["plotly_fig"] is not None:
            st.plotly_chart(chat["plotly_fig"], width='content')

# --- INPUT UTILISATEUR ---
if user_input := st.chat_input("Ex: Quel est le score du RHDP à Divo ?"):
    st.session_state.context = None
    st.session_state.options = None
    st.session_state.history.append({"role": "user", "content": user_input})
    st.session_state.count += 1
    st.rerun()

# --- TRAITEMENT DU CHAT ---
if st.session_state.history and st.session_state.history[-1]["role"] == "user":
    query = st.session_state.history[-1]["content"]
    placeholder = st.empty()
    placeholder.markdown('<div class="typing"><div class="dot"></div><div class="dot"></div><div class="dot"></div></div>', unsafe_allow_html=True)

    intent = agent.route_intent(query)

    # --- INTENTS ---
    if intent in ["GREETING", "SECURITY", "OFFTOPIC"]:
        messages = {
            "GREETING": agent.generate_greeting(query),
            "SECURITY": "🔒 Requête bloquée pour raisons de sécurité.",
            "OFFTOPIC": "❌ Hors sujet par rapport aux élections législatives 2025."
        }
        placeholder.empty()
        st.session_state.history.append({"role":"assistant", "content": messages[intent]})
        st.rerun()

    elif intent == "DATA":
        # Ambiguïté
        if not st.session_state.context and not st.session_state.options:
            ambiguities = agent.check_ambiguity(query)
            if ambiguities:
                placeholder.empty()
                if isinstance(ambiguities, dict):
                    st.session_state.context = ambiguities['new']
                    st.session_state.history[-1]["content"] = f"Résultats pour {ambiguities['new']}"
                else:
                    st.session_state.options = ambiguities
                    st.session_state.last_query = query
                    st.session_state.ambiguous_term = query.split()[-1]
                    display_ambiguity_options()
                    st.stop()
        if st.session_state.options:
            display_ambiguity_options()
            st.stop()

        # Exécution SQL
        context_to_use = st.session_state.context
        placeholder.markdown('<div class="typing"><div class="dot"></div><div class="dot"></div><div class="dot"></div></div>', unsafe_allow_html=True)
        df, sql, error = agent.validate_and_execute(query, context_to_use)

        if error:
            placeholder.empty()
            st.session_state.history.append({"role": "assistant", "content": f"⚠️ {error}", "sql": sql})
        elif df is None or df.empty:
            placeholder.empty()
            st.session_state.history.append({"role": "assistant", "content": "Désolé, aucune donnée trouvée.", "sql": sql})
        else:
            # Narration + Graphique
            narration = "".join([chunk for chunk in agent.generate_narrative(query, df)])
            placeholder.empty()
            plotly_fig = None
            query_up = query.upper()

            graph_keywords = ["HISTOGRAMME", "GRAPHIQUE", "BARRE", "REPARTITION", "CAMEMBERT", "PIE", "TOP"]
            if any(k in query_up for k in graph_keywords) and len(df) > 1:
                try:
                    num_cols = df.select_dtypes(include=['number']).columns.tolist()
                    cat_cols = df.select_dtypes(include=['object']).columns.tolist()
                    if len(num_cols) >= 1 and len(cat_cols) >= 1:
                        x_label = num_cols[0].replace('_', ' ').upper()
                        y_label = cat_cols[0].replace('_', ' ').upper()
                        if "CAMEMBERT" in query_up or "PIE" in query_up:
                            fig = px.pie(df.head(10), values=num_cols[0], names=cat_cols[0], title=f"RÉPARTITION : {y_label}")
                        else:
                            fig = px.bar(df.head(15), x=num_cols[0], y=cat_cols[0],
                                         orientation='h', title=f"ANALYSE : {x_label} PAR {y_label}",
                                         labels={num_cols[0]: x_label, cat_cols[0]: y_label},
                                         color=num_cols[0], color_continuous_scale="Viridis")
                            fig.update_layout(yaxis={'categoryorder':'total ascending'})

                        fig.update_layout(
                            paper_bgcolor='rgba(0,0,0,0)',
                            plot_bgcolor='rgba(0,0,0,0)',
                            font=dict(color="white"),
                            margin=dict(l=20, r=20, t=50, b=20),
                            showlegend=("CAMEMBERT" in query_up)
                        )
                        if "CAMEMBERT" not in query_up:
                            fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='rgba(255,255,255,0.1)')
                            fig.update_yaxes(showgrid=False)
                        plotly_fig = fig
                except Exception as e:
                    logger.error(f"Erreur rendu graphique : {e}")

            st.session_state.history.append({"role": "assistant", "content": narration, "sql": sql, "plotly_fig": plotly_fig})

        # Nettoyage
        st.session_state.context = None
        st.session_state.options = None
        st.session_state.ambiguous_term = None
        st.rerun()
