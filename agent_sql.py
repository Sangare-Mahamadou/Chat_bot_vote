import pandas as pd, duckdb, ollama, re, json, os, logging, unicodedata
from typing import Optional, Tuple, List, Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ElectionSecurityError(Exception): pass
class ElectionDataError(Exception): pass

class ElectionAgent:
    def __init__(self, db_path: str, schema_path: str):
        self.db_path = os.path.normpath(db_path)
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Base de données non trouvée : {self.db_path}")
            
        self.con = duckdb.connect(self.db_path)
        with open(schema_path, 'r', encoding='utf-8') as f:
            self.schema = json.load(f)
        
        # Récupération des configurations du schéma
        self.security_rules = self.schema.get("security_rules", {})
        self.default_limit = self.security_rules.get("auto_limit", 100)
        self.max_allowed = self.security_rules.get("max_rows", 100)
        
        # Utilisation des vues autorisées du schéma
        self.allowed_views = [v["view_name"] for v in self.schema.get("allowed_views", [])]
        self.allowed_tables = [t["table_name"] for t in self.schema.get("allowed_tables", [])]
        self.allowed_objects = self.allowed_views + self.allowed_tables
        
        # Utilisation directe des aliases du schéma
        self.aliases = {
            'partis': self.schema.get("common_aliases", {}).get("partis", {}),
            'regions': self.schema.get("common_aliases", {}).get("regions", {}),
            'columns': self.schema.get("column_aliases", {})
        }
        
        # Cache pour les normalisations
        self._cache = {}

    def _best_view_for_query(self, query: str) -> str:
        """Détecte la vue la plus adaptée depuis le schéma"""
        query_lower = query.lower()
        
        # Mapping des mots-clés aux vues (utilise les descriptions du schéma)
        view_keywords = {
            'vw_winners': ['gagné', 'élu', 'vainqueur', 'siege', 'winner'],
            'vw_turnout': ['participation', 'taux'],
            'vw_party_stats': ['parti', 'groupe', 'coalition', 'statistique parti'],
            'vw_region_stats': ['région', 'region', 'statistique région'],
            'vw_candidate_performance': ['performance', 'classement', 'rang', 'top', 'meilleur'],
            'vw_rag_search': ['recherche', 'cherche', 'trouve', 'localise'],
            'vw_party_search': ['alias', 'recherche parti'],
        }
        
        for view, keywords in view_keywords.items():
            if any(kw in query_lower for kw in keywords) and view in self.allowed_views:
                return view
        
        return "vw_results_clean"

    def normalize_query(self, query: str) -> str:
        """Normalisation ultra-optimisée utilisant le schéma"""
        query = query.upper()
        
        partis_map = self.schema.get("common_aliases", {}).get("partis", {})
        
        # Création d'une liste plate d'alias triée par longueur (plus long en premier)
        flat_aliases = []
        for std, aliases in partis_map.items():
            for a in aliases:
                if a.upper() != std.upper():
                    flat_aliases.append((a.upper(), std.upper()))
        
        flat_aliases.sort(key=lambda x: len(x[0]), reverse=True)        

        for alias, std in flat_aliases:
            pattern = r'\b' + re.escape(alias) + r'\b'
            query = re.sub(pattern, std, query)
        
        return query.strip()

    def route_intent(self, query: str) -> str:
        """Détection d'intention rapide"""
        query_up = query.upper()
        
        # Sécurité basée sur le schéma
        forbidden = {k.upper() for k in self.security_rules.get("forbidden_keywords", []) if k.upper() != "INDEPENDANT"}
    
        # Vérifie chaque mot de la requête
        for word in re.findall(r"[A-ZÉÈÊÀÇ]+", query_up):
            if word in forbidden:
                return "SECURITY"

        mots_dangereux = {
                "SUPPRIMER", "SUPPRIME","SUPRIMER","SUPRIME","EFFACE", "EFFACER", "DETRUIRE", "DETRUIT",
                "MODIFIER", "MODIFIE", "AJOUTER", "AJOUTE", "CHANGER", 
                "CHANGE", "REMPLACE", "REMPLACER", "CREE", "CREER",
                "VIDER", "NETTOYER", "ENLEVER", "RETIRER"}
            
        if any(mot in query_up for mot in mots_dangereux):
            return "SECURITY"

        # Salutations
        if any(kw in query_up for kw in ["BONJOUR", "BONSOIR", "SALUT", "HELLO", "COUCOU"]):
            return "GREETING"
        
        # Questions hors sujet

        mots_hors_sujet = [
            "MÉTÉO", "CLIMAT", "FOOTBALL", "SPORT", "HISTOIRE", "GÉOGRAPHIE", 
            "SCIENCE", "TECHNOLOGIE", "MUSIQUE", "CINÉMA", "FILM", "SANTÉ",
            "CUISINE", "RECETTE", "VOYAGE", "VACANCES", "POLITIQUE INTERNATIONALE",
            "ÉCONOMIE MONDIALE", "ENVIRONNEMENT GLOBAL","TEMPS","PRESIDENT","COMMENT FAIRE"
        ]

        if any(mot in query_up for mot in mots_hors_sujet):
            return "OFFTOPIC"
        
        # Données électorales
        data_keywords = ["VOIX", "SCORE", "PARTICIPATION", "SIEGE", "ELU", 
                        "CANDIDAT", "PARTI", "REGION", "TOP", "CLASSEMENT"]
        if any(kw in query_up for kw in data_keywords):
            return "DATA"
        return "DATA"

    def generate_sql(self, user_query: str, context_choice: Optional[str] = None, 
                 error_feedback: Optional[str] = None) -> str:
        col_desc = json.dumps(self.schema.get("column_descriptions", {}), indent=1)
        
        context_instruction = ""
        if context_choice:
            context_instruction = f"NOTE : L'utilisateur a sélectionné '{context_choice}'. Utilise cette valeur exacte dans la clause WHERE sur la colonne la plus adaptée (region ou circonscription)."

        system_prompt = f"""
        Tu es un moteur SQL strict pour DuckDB expert des élections 2025 en Côte d'Ivoire.
        SCHEMA : {col_desc}
        
        RÈGLES CRITIQUES :
        1. RÉPOND UNIQUEMENT PAR LE CODE SQL PUR.
        5. Pour le calcule des totaux utilise toujours SUM() sur la colonne concernée depuis la vue vw_results_clean, même si la question ne demande pas explicitement un total. Par exemple, pour "Combien de voix pour le RHDP dans la région X?" : SELECT SUM(voix) FROM vw_results_clean WHERE region = 'X' AND parti_normalized = 'RHDP'
        7. NE PAS INVENTER de noms de colonnes qui n'existent pas dans le schéma.
        8. PDCI-RDA signifie :  Parti Démocratique de Côte d'Ivoire-Rassemblement démocratique Africain. 
        9. Pour "Compare les résultats du A et du B" : SELECT parti_standardized, SUM(voix) as total_voix, SUM(est_elu) as total_sieges FROM vw_results_clean WHERE parti_standardized IN ('A', 'B') GROUP BY parti_standardized
        10. NE JAMAIS FILTRER PAR RÉGION si l'utilisateur demande une ville (circonscription), et vice-versa.
        11. RECHERCHE : Utilise toujours pour les comparaisons.
        

        INSTRUCTIONS SPÉCIFIQUES :
    
        A) Pour compter les communes ou sous-prefectures ou communes et sous-prefecture dans une circonscription :
        - Découper chaque ligne de la colonne 'circonscription' par virgule
        - Prendre le dernier élément de la liste de chaque ligne decoupé obtenue
        - Si le dernier élément est 'COMMUNE' : compter tous les éléments précédents cela te donne le nombre de circonscription qui sont commune.
        - Si le dernier élément est 'SOUS-PREFECTURE' : compter tous les éléments précédents cela te donne le nombre de circonscription qui sont sous-prefecture
        - Si le dernier élément est 'COMMUNES ET SOUS-PREFECTURES' : compter tous les éléments précédents, cela te donne le nombre des circonscription qui sont communes et sous-prefecture.
        - Utiliser cette logique : 
            CASE 
                WHEN UPPER(SPLIT_PART(circonscription, ',', -1)) LIKE '%COMMUNE%' 
                THEN array_length(string_to_array(circonscription, ',')[:-1])
                ELSE 0 
            END AS nombre_communes

        B) Pour trouver les adversaires d'un candidat X :
        - Trouver le d'abord le candidat X
        - Ensuite trouve la circonscription du candidat X
        - Enfin sélectionner TOUS les candidats de cette circonscription SAUF X
        - Modèle : SELECT candidat FROM vw_results_clean WHERE circonscription = (SELECT circonscription FROM vw_results_clean WHERE candidat = UPPER('X')) AND candidat != 'X'

        C) Pour savoir qui a gagné dans une circonscription X :
        - Chercher dans 'vw_results_clean' où circonscription = 'X' et est_elu = 1
        - Modèle : SELECT candidat, parti FROM vw_results_clean WHERE UPPER(circonscription) = UPPER('X') AND est_elu = 1

        D) Pour les données par région (inscrits, votants,bulletins_blancs, bulletins_nuls etc.) :
        - Utiliser SUM() sur les colonnes existantes pour determiner le nombre ou le nombre total: inscrits, votants, voix
        - Modèle : SELECT SUM(inscrits), SUM(bulletins_nuls), SUM(bulletins_blancs), SUM(suffrages_exprimes), SUM(votants), (SUM(votants)/SUM(inscrits))*100 FROM vw_results_clean WHERE region = 'X'

        CONTEXTE : {context_instruction}

        {f"CORRIGE CETTE ERREUR PRÉCÉDENTE : {error_feedback}" if error_feedback else ""}
        SQL pur uniquement. Pas de texte, pas de bla-bla.
        """
        response = None
        sql = "SELECT 'Erreur de génération' as info;"

        # Définition du prompt selon le cas
        if context_choice:
            # On ne force plus la colonne, on laisse le LLM choisir entre region et circonscription
            prompt_final = f"Génère le SQL pour le choix : '{context_choice}'. Filtre sur la colonne adéquate."
        else:
            prompt_final = f"Question : {user_query}"

        try:
            # APPEL UNIQUE À OLLAMA
            response = ollama.generate(
                model='llama3', 
                system=system_prompt, 
                prompt=prompt_final, 
                options={"temperature": 0, "num_predict": 150, "stop": [";", "Note:"]}
            )
            
            if response and 'response' in response:
                # Nettoyage sécurisé
                raw_sql = response['response']
                sql = re.sub(r'```sql|```', '', raw_sql).strip()
                
                # On s'assure de ne garder que la partie SQL
                start_idx = sql.upper().find("SELECT")
                if start_idx != -1:
                    sql = sql[start_idx:]

        except Exception as e:
            # En cas de timeout ou crash d'Ollama
            print(f"Erreur Ollama : {e}")
            if context_choice:
                sql = f""" SELECT * FROM vw_results_clean WHERE circonscription = '{context_choice}'"""
        return sql
            

    def check_ambiguity(self, query: str) -> Any:
        """Vérification d'ambiguïté avec extraction d'entités résiliente et floue."""
        import re
        from typing import Optional, Tuple

        # 1. NETTOYAGE ET NORMALISATION PRÉLIMINAIRE
        def clean_input(text: str) -> str:
            # Enlève les accents pour la comparaison (utile pour Tonkpi/tonkpi)
            import unicodedata
            text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
            return text.upper().strip()
        
        query = clean_input(query)
        # 2. EXTRACTION D'ENTITÉS AVEC PATTERNS SOUPLES
        def extract_entity(query_text: str) -> Tuple[Optional[str], Optional[str]]:
            q = query_text.upper()
            
            patterns = {
                "region": [r"R[EÉ]GION[:\s]+(?:D[EUA']\s+)?([A-Z\s\-\'']+)"],
                "circonscription": [
                    r"COMMUNE\s+(?:D[EUA']\s+)?([A-Z\s\-\'']+)",
                    r"CIRCONSCRIPTION\s+(?:D[EUA']\s+)?([A-Z\s\-\'']+)",
                    r"À\s+([A-Z\s\-\'']+)(?:\s+COMMUNE)?",
                    r"POUR\s+([A-Z\s\-\'']+)"
                ],
                "candidat": [
                    r"CANDIDAT\s+([A-Z\s\-\'']+)",
                    r"M(?:\.|ONSIEUR|ME|ADAME)\s+([A-Z\s\-\'']+)"
                ]
                
            }
            
            for e_type, p_list in patterns.items():
                for p in p_list:
                    m = re.search(p, q, re.IGNORECASE)
                    if m: return m.group(1).strip(), e_type
            
            # Fallback : On prend les mots restants après avoir enlevé les stop-words
            stop_words = {"DANS", "POUR", "AVEC", "SUR", "AUX", "DES", "DU", "DE", "LA", "LE", "LES", "EST", "A", "À", "ET", "RÉSULTATS", "ÉLECTIONS"}
            words = [w for w in q.replace("?", "").split() if w not in stop_words and len(w) >= 2]
            return (words[-1] if words else None), None

        found_keyword, entity_type = extract_entity(query)
        if not found_keyword: return None

        # 3. RECHERCHE SQL MULTI-STRATÉGIE (Exact -> Flou -> Contient)
        # On utilise ILIKE pour l'insensibilité à la casse et levenshtein pour les fautes
        found_keyword_sql = found_keyword.replace("'", "''")
        options = []
        
        # Colonnes à scanner
        cols = [("region", "Région"), ("circonscription", "Circonscription"), ("candidat", "Candidat")]
        if entity_type: # Priorité au type détecté
            cols = [(entity_type, entity_type.capitalize())]

        for col, label in cols:
            try:
                # La puissance de DuckDB : Recherche par mot-clé et distance
                sql = f"""
                    SELECT DISTINCT {col} as val
                    FROM vw_results_clean
                    WHERE {col} IS NOT NULL AND (
                        {col} ILIKE '%{found_keyword_sql}%'
                        OR levenshtein({col}, UPPER('{found_keyword_sql}')) <= 2
                        OR EXISTS (
                            SELECT 1 FROM unnest(string_split({col}, ' ')) as t(part)
                            WHERE t.part ILIKE UPPER('{found_keyword_sql}')
                        )
                    )
                    ORDER BY length(val) ASC
                    LIMIT 3
                """
                res = self.con.execute(sql).df()
                for v in res['val'].tolist():
                    options.append(f"{label}: {v}")
            except: continue

        # 4. DÉDUPLICATION ET RETOUR
        unique_options = list(dict.fromkeys(options))
        
        if len(unique_options) == 1:
            parts = unique_options[0].split(': ')
            return {
                "status": "corrected",
                "old": found_keyword,
                "new": parts[1],
                "column": parts[0].lower()
            }
        
        return unique_options if len(unique_options) > 1 else None
    

    def validate_and_execute(self, user_question: str, context_choice: Optional[str] = None) -> Tuple[Optional[pd.DataFrame], str, Optional[str]]:
        """Exécution avec auto-correction (Feedback Loop)"""
        
        def run_process(query, context, feedback=None):
            sql = self.generate_sql(query, context, error_feedback=feedback)

            # Nettoyage du SQL
            sql = sql.replace("```sql", "").replace("```", "")
            start_index = sql.upper().find("SELECT")
            if start_index != -1:
                sql = sql[start_index:].strip()
            sql = sql.split(';')[0] + ';'

            # Sécurités de base
            sql_upper = sql.upper()
            if not sql_upper.startswith("SELECT"):
                return None, sql, "🔒 Requête bloquée pour raisons de sécurité."
            
            if not any(view in sql_upper for view in [v.upper() for v in self.allowed_views]):
                return None, sql, "⚠️ Vue non autorisée."

            # if "LIMIT" not in sql_upper:
            #     sql = f"{sql.rstrip(';')} LIMIT {self.default_limit};"

            try:
                df = self.con.execute(sql).df()
                return df, sql, None
            except Exception as e:
                return None, sql, str(e)

        # --- ÉTAPE 1 : Premier essai ---
        df, final_sql, error = run_process(user_question, context_choice)

        # --- ÉTAPE 2 : Auto-correction si erreur ---
        if error and ("column" in error.lower() or "syntax" in error.lower() or "parser" in error.lower()):
            # On retente une fois avec le feedback d'erreur
            df_corr, sql_corr, error_corr = run_process(user_question, context_choice, feedback=error)
            if not error_corr:
                return df_corr, sql_corr, None
            else:
                error = error_corr 

        # --- ÉTAPE 3 : Traduction de l'erreur pour l'utilisateur ---
        if error:
            error_msg = error.lower()
            if "column" in error_msg:
                user_msg = "Donnée non disponible dans ce format."
            elif "syntax" in error_msg:
                user_msg = "Erreur de formulation technique."
            else:
                user_msg = "Aucune donnée trouvée."
            return None, final_sql, user_msg

        return df, final_sql, None

    def generate_narrative(self, question: str, df: pd.DataFrame):
        """Génération narrative streamée optimisée"""
        if df is None or df.empty:
            yield "Aucune donnée ne correspond à votre recherche."
            return
        
        data_preview = df.head(3).to_string(index=False)

        system_prompt = f"""Tu es l'assistant électoral 2025. 
        Données ({len(df)} lignes): {data_preview}
        Réponds UNIQUEMENT basé sur les données : {data_preview}
        Interdiction formelle d'utiliser tes connaissances externes.
        fait au maximum 1 phrase très concise.
        Réponds en français simple et clair

        RÈGLES STRICTES :
        2. Ne fais jamais de spéculations sur le futur (date supérieure à 2025) ni sur le passé (date inférieure à 2025).
        3. Utilise uniquement les chiffres présents dans le tableau fourni.
        4. Si l'utilisateur demande le RHDP, réponds sur le RHDP uniquement.
        5. Formate les totaux sans décimales (donne en entier).
        6. Reformule la question de l'utilisateur dans ta réponse pour plus de clarté, mais fais-le de manière concise et naturelle, sans dis tes choses qui n'ont rien avoir avec la question. Par exemple, si la question est "Combien de voix pour le RHDP dans la région X?" tu peux répondre "Le RHDP a obtenu 15000 voix dans la région X." mais pas "Dans la région X, le RHDP a obtenu 15000 voix, ce qui représente une augmentation par rapport à 2020." (car tu n'as pas les données de 2020 et tu ne dois pas faire de spéculation).
        """
                
        try:
            stream = ollama.generate(model='mistral', system=system_prompt, 
                                   prompt=question, stream=True, options={"temperature": 1})
            for chunk in stream:
                if 'response' in chunk:
                    yield chunk['response']
        except:
            yield f"Données récupérées: {len(df)} résultat(s)."

    from functools import lru_cache
    # Dans ta classe ElectionAgent
    @lru_cache(maxsize=100)
    def generate_greeting(self, user_query: str) -> str:
        """Salutation avec stats du schéma"""
        self.schema.get("database_info", {}).get("statistics", {})
        system_prompt = f"""Tu es l'assistant des élections législatives 2025.
        Réponds en 1 phrases maximum, amical et professionnel.
        """
        
        try:
            return ollama.generate(model='mistral', system=system_prompt, 
                                 prompt=user_query, options={"temperature": 0})['response'].strip()
        except:
            return "Bonjour ! Assistant électoral 2025. Analyse des résultats législatifs."

    def close(self):
        if hasattr(self, 'con'):
            self.con.close()