# ***Challenge IA - ARTEFACT***

## 🗳️ ***Assistant Électoral CI 2025***

### ***Présentation***
Application de Chat IA permettant d'interroger les résultats officiels des législatives 2025 en Côte d'Ivoire. 
L'agent utilise un moteur **Text-to-SQL** sur base **DuckDB** pour garantir des réponses factuelles et précises.

### ***Fonctionnalités (Niveau 1-3 validés)***
- **SQL Agent** : Traduction naturelle en requêtes SQL sécurisées (SELECT uniquement).
- **Graphiques** : Génération à la demande (Barres, Camemberts, Histograme) via Plotly.
- **Désambiguïsation** : Gestion intelligente des localités ou candidats multiples (ex: Tiapoum).
- **Sécurité** : Protection contre l'injection SQL et filtrage des questions hors-sujet.
- **UI/UX** : Interface Streamlit moderne avec thématique électorale.

### ***Installation***
1. `pip install -r requirements.txt`
2. Lancer l'ingestion : Exécuter les notebooks dans `/notebooks` pour l'exaction et génération de la base DuckDB.
    - Executer les celules de Extraction_Traitement.ipynb pour l'extraction et le traitement des données
    - Executer les celules de database_setup.ipynb pour la créeation des vues , du schema de la base et créeation de la base DuckDB (election_ci.db)
3. Lancer l'app : `streamlit run src/appN3.py` pour l'intéragir avec le chat bot mis en place

### ***Stack Technique***
- **Extraction** : pdfplumber / pandas
- **Base de données** : DuckDB
- **LLM Orchestration** : Ollama (Mistral) / LangChain concept
- **Frontend** : Streamlit + CSS personnalisé

### ***Structure***

PROJET_VOTECI25/
├── src/
|   ├──data/
│   |    ├── database/ election_ci.db      # Base DuckDB générée
│   |    └──              
│   ├── appN3.py                           # Votre code appN3.py
│   ├── agent_sql.py                       # Votre code agent_sql_opti_Copie2.py
│   ├── schema_for_agent.json              # Schéma et métadonnées
│   ├── style.css                          # Design UI
|   └── notebooks/
│        ├── 01_extraction.ipynb           # Votre Extraction_Traitement.ipynb
│        └── 02_setup_db.ipynb             # Votre 02_database_setup.ipynb
├── requirements.txt                       # Liste des dépendances (pandas, duckdb, streamlit, etc.)
├── README.md                               # Guide d'installation et documentation
├── EDAN_2025_RESULTAT_NATIONAL_DETAILS.pdf    # Source originale                        
└── RAPPORT_TECHNIQUE.pdf                  # Le rapport de synthèse