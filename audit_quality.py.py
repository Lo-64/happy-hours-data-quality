import pandas as pd

# 1. Chargement de la table centrale
chemin = r"C:\Users\LoicM\OneDrive\Documents\STAT MA 1\Data mining\projet_dagster\product_snapshots_bruxelles_2024-01_2026-08.parquet"
print("Chargement des données en cours...")
df = pd.read_parquet(chemin)
total_lignes = len(df)
print(f"Total de lignes chargées : {total_lignes:,}\n")

# 2. Check valeurs manquantes 
print("=" * 60)
print("VALEURS MANQUANTES (NULLS)")
print("=" * 60)
nulls = df.isnull().sum() # Compte le nombre de cases manquantes du tableau 
nulls_pct = (nulls / total_lignes) * 100
rapport_nulls = pd.DataFrame({"Valeurs Manquantes": nulls, "Pourcentage (%)": nulls_pct.round(2)})
print(rapport_nulls[rapport_nulls["Valeurs Manquantes"] > 0])
if (nulls == 0).all():
    print("Aucune valeur manquante détectée.")

# 3. Pour colonnes de chiffres: calcul de la valeur min,max et de la médiane.
print("\n" + "=" * 60)
print("BORNES NUMÉRIQUES (MIN / MEDIANE / MAX)")
print("=" * 60)
colonnes_num = [
    "quantity_put_on_sale_start_of_day", 
    "quantity_sold", 
    "quantity_end_sales", 
    "origin_quantity", 
    "old_price", 
    "new_price", 
    "unit_weight"
]
stats = df[colonnes_num].describe().T[["min", "50%", "max"]]
stats.columns = ["Min", "Médiane", "Max"]
print(stats)

# 4. Premiers tests d'anomalie
print("\n" + "=" * 60)
print("DÉTECTION DES ANOMALIES DE BORNES STRICTES")
print("=" * 60)
regles = [
    ("Stock début de journée négatif (< 0)", "quantity_put_on_sale_start_of_day", df["quantity_put_on_sale_start_of_day"] < 0),
    ("Ventes négatives (< 0)", "quantity_sold", df["quantity_sold"] < 0),
    ("Stock fin de journée négatif (< 0)", "quantity_end_sales", df["quantity_end_sales"] < 0),
    ("Quantité d'origine négative (< 0)", "origin_quantity", df["origin_quantity"] < 0),
    ("Prix remisé nul ou négatif (new_price <= 0)", "new_price", df["new_price"] <= 0),
    ("Prix d'origine nul ou négatif (old_price <= 0)", "old_price", df["old_price"] <= 0),
    ("Poids négatif (unit_weight < 0)", "unit_weight", df["unit_weight"] < 0)
]

for nom_regle, nom_colonne, condition in regles:
    nb_lignes = condition.sum()  # Compte le nombre de lignes en anomalie. 
    pct = (nb_lignes / total_lignes) * 100 # Calcules le pourcentages de lignes en anomalie sur le nombre de lignes totalles. 
    print(f"\n- {nom_regle} : {nb_lignes:,} lignes ({pct:.3f} %)")
    if nb_lignes > 0: # S'il y a au moins une anomalie, on liste les valeurs exactes et leur fréquence
        valeurs_anormales = df.loc[condition, nom_colonne].value_counts() # Isole uniquement les cases en anomalie sur cette colonne précise et compte la fréquence de chaque valeur anormale trouvée.
        print("  Détail des valeurs trouvées :")
        for valeur, occurrences in valeurs_anormales.items(): # On print le tuple valeurs anormales
            print(f"    * Valeur '{valeur}' -> apparue {occurrences:,} fois")
        if len(valeurs_anormales) > 10:
            print(f"    * ... et {len(valeurs_anormales) - 10} autres valeurs distinctes.")

# 5. Prend la date la plus ancienne et la plus récente de chaque colonne temporelle 
# pour vérifier qu'aucune date n'est une abération
print("\n" + "=" * 60)
print("BORNES TEMPORELLES (PLAGES DE DATES)")
print("=" * 60)
colonnes_dates = ["snapshot_date", "scan_date", "expiry_date"]
for col in colonnes_dates:
    print(f"- {col} -> Min : {df[col].min()} | Max : {df[col].max()}")

# 6. Liste toutes les valeurs distinctes trouvées et leur nombre d'apparition pour vérifier qu'on a bien uniquement "DLC" et "DDM"
print("\n" + "=" * 60)
print("TYPES DE DATE (date_type)")
print("=" * 60)
print(df["date_type"].value_counts(dropna=False))

print("=" * 65)
print("1. ÉQUATION DE STOCK : (DÉBUT - VENDU == FIN)")
print("=" * 65)
# On calcule l'écart théorique : (Stock début - Ventes) - Stock fin
ecart_stock = (df["quantity_put_on_sale_start_of_day"] - df["quantity_sold"]) - df["quantity_end_sales"]
incoherences_stock = (ecart_stock != 0)
nb_inc_stock = incoherences_stock.sum()

print(f"- Lignes avec rupture de l'équation de stock : {nb_inc_stock:,} ({nb_inc_stock / total_lignes * 100:.3f} %)")
if nb_inc_stock > 0:
    print("  Exemples d'écarts observés (quantité_théorique - quantité_réelle) :")
    for ecart, count in ecart_stock[incoherences_stock].value_counts().items():
        print(f"    * Écart de {ecart:.0f} unité(s) -> {count:,} fois")

# Vérification si les ventes ont dépassé le stock mis en vente
surventes = df["quantity_sold"] > df["quantity_put_on_sale_start_of_day"]
nb_surventes = surventes.sum()
print(f"- Ventes supérieures au stock initial (survente) : {nb_surventes:,} ({nb_surventes / total_lignes * 100:.3f} %)")

# Vérification si le stock mis en vente dépasse la quantité reçue d'origine
sur_mise_en_vente = df["quantity_put_on_sale_start_of_day"] > df["origin_quantity"]
nb_sur_mise = sur_mise_en_vente.sum()
print(f"- Mis en vente > Quantité d'origine reçue : {nb_sur_mise:,} ({nb_sur_mise / total_lignes * 100:.3f} %)")

print("\n" + "=" * 65)
print("2. COHÉRENCE DES PRIX : (NEW_PRICE <= OLD_PRICE)")
print("=" * 65)
prix_anormal = (df["new_price"] > df["old_price"]) & (df["old_price"] > 0) # On a déjà compté l'naomalie old_price=0 au niveau_1, ici on isole l'anomalie de remise
nb_prix_anormal = prix_anormal.sum()
print(f"- Produit remisé plus cher que l'original : {nb_prix_anormal:,} ({nb_prix_anormal / total_lignes * 100:.3f} %)")

print("\n" + "=" * 65)
print("3. RÈGLE SANITAIRE : DLC PÉRIMÉE EN RAYON OU VENDUE")
print("=" * 65)
date_rayon = pd.to_datetime(df["snapshot_date"]).dt.tz_localize(None).dt.floor("D")
date_peremption = pd.to_datetime(df["expiry_date"]).dt.tz_localize(None).dt.floor("D")

# Filtre de base : DLC dépassée le jour du snapshot
est_dlc_expiree = (df["date_type"] == "DLC") & (date_rayon > date_peremption)

# Cas 1 : Proposé aux clients alors que la DLC est dépassée
dlc_en_rayon = est_dlc_expiree & (df["quantity_put_on_sale_start_of_day"] > 0)
nb_dlc_rayon = dlc_en_rayon.sum()
print(f"- Produits DLC proposés en rayon après expiration : {nb_dlc_rayon:,} ({nb_dlc_rayon / total_lignes * 100:.3f} %)")

# Cas 2 : Réellement vendu alors que la DLC est dépassée (faute grave)
dlc_vendue = est_dlc_expiree & (df["quantity_sold"] > 0)
nb_dlc_vendue = dlc_vendue.sum()
print(f"- Produits DLC VENDUS après expiration : {nb_dlc_vendue:,} ({nb_dlc_vendue / total_lignes * 100:.3f} %)")


# =============================================================
# NIVEAU 3 : INTÉGRITÉ RELATIONNELLE (PRODUITS ORPHELINS)
# =============================================================
print("\n" + "=" * 65)
print("NIVEAU 3 : INTÉGRITÉ RELATIONNELLE (CATALOGUE RÉFÉRENCE)")
print("=" * 65)

# 1. On charge le catalogue maître de la même façon que ta première table
chemin_ref = r"C:\Users\LoicM\OneDrive\Documents\STAT MA 1\Data mining\projet_dagster\product_reference.parquet"
df_ref = pd.read_parquet(chemin_ref)

# 2. On extrait l'ensemble des codes EAN officiels et valides
eans_officiels = set(df_ref["ean_code"].dropna())

# 3. On repère dans 'df' les lignes dont l'EAN n'existe PAS dans le catalogue central (df_ref)
lignes_orphelines = ~df["ean_code"].isin(eans_officiels)
nb_orphelines = lignes_orphelines.sum()
pct_orphelines = (nb_orphelines / total_lignes) * 100

# 4. On compte combien d'articles différents cela représente
nb_produits_inconnus = df.loc[lignes_orphelines, "ean_code"].nunique()

print(f"- Lignes de stock avec EAN inconnu au catalogue : {nb_orphelines:,} ({pct_orphelines:.3f} %)")
print(f"- Nombre de produits distincts non référencés : {nb_produits_inconnus:,}")

# =============================================================
# NIVEAU 4 : ANALYSE STATISTIQUE CONDITIONNELLE & VALIDATION CATÉGORIE
# =============================================================
print("\n" + "=" * 65)
print("NIVEAU 4 : ANALYSE STATISTIQUE RIGOUREUSE (CATÉGORIES & RATIOS)")
print("=" * 65)

# -------------------------------------------------------------
# 4.1. CHECK DE L'ERREUR D'UNITÉ (GRAMMES VS KILOS)
# -------------------------------------------------------------
print("1. VALIDATION CROISÉE DES POIDS AVEC LE CATALOGUE :")



# Associe à chaque ligne de stock son poids officiel déclaré dans le catalogue via leur code EAN commun.
df_poids = df[["ean_code", "product_name", "unit_weight"]].merge(
    df_ref[["ean_code", "unit", "unit_quantity"]], on="ean_code", how="left"
)

masque_valide = (df_poids["unit_weight"] > 0) & (df_poids["unit_quantity"] > 0) # Élimine les articles sans poids ou à 0 pour éviter toute division par zéro ou valeur indéfinie.
df_valid_weight = df_poids[masque_valide].copy() #  Crée un sous-tableau propre contenant uniquement les lignes exploitables. 

# Divise le poids affiché en rayon par le poids officiel. Un résultat de 1.0 indique la même unité ; un résultat de 1000.0 prouve une saisie en grammes 
df_valid_weight["ratio"] = (df_valid_weight["unit_weight"] / df_valid_weight["unit_quantity"]).round(1)

# On compte les lignes où le ratio est exactement de 1 000 (saisie en grammes)
lignes_ratio_1000 = (df_valid_weight["ratio"] >= 950) & (df_valid_weight["ratio"] <= 1050)
nb_ratio_1000 = lignes_ratio_1000.sum()
pct_ratio_1000 = (nb_ratio_1000 / total_lignes) * 100

print(f"- Lignes avec ratio ~1 000 (preuve de saisie en grammes) : {nb_ratio_1000:,} ({pct_ratio_1000:.3f} %)")
print("Attention les autres rations comme 4,6,12 peuvent être des produits vendus en packs/lots. Ratios de 0.5, 0.25 peuvent être des portions vendues à la coupe ou demi-formats.")

print(" Distribution des principaux ratios observés :")
top_ratios = df_valid_weight["ratio"].value_counts().head(5)
for ratio_val, count in top_ratios.items():
    interpretation = "Poids identique (normal)" if ratio_val == 1.0 else ("Facteur 1 000 (Grammes)" if ratio_val == 1000.0 else "Autre facteur")
    print(f"    * Ratio {ratio_val:>7.1f} -> {count:,} lignes ({interpretation})")


# -------------------------------------------------------------
# 4.2. PRIX ANORMAUX PAR CATÉGORIE (MÉTHODE DE TUKEY : Q3 + 3 * IQR)
# -------------------------------------------------------------
print("\n2. PRIX ANORMAUX PAR CATÉGORIE:")

# Calcul de Q1 (25%), Q3 (75%) et IQR (=écart interquartile) par catégorie
stats_cat = df.groupby("category")["old_price"].quantile([0.25, 0.75]).unstack()
stats_cat["IQR"] = stats_cat[0.75] - stats_cat[0.25]
# Seuil Tukey pour borner le prix maximum plausible au-delà duquel un article est une anomalie extrême: Q3 + 3 * IQR
stats_cat["seuil_extreme"] = stats_cat[0.75] + (3.0 * stats_cat["IQR"])

# On associe le seuil de sa catégorie à chaque ligne
df_prix = df[["category", "product_name", "old_price", "new_price"]].merge(
    stats_cat[["seuil_extreme"]], on="category", how="left"
)

# On filtre les prix qui explosent le seuil de leur propre catégorie
outliers_prix = df_prix[df_prix["old_price"] > df_prix["seuil_extreme"]]
nb_outliers_prix = len(outliers_prix)
pct_outliers_prix = (nb_outliers_prix / total_lignes) * 100

print(f"- Lignes en anomalie extrême par rapport à leur rayon : {nb_outliers_prix:,} ({pct_outliers_prix:.3f} %)")

print("  Top 5 des anomalies de prix les plus violentes relatives à leur catégorie :")
outliers_prix = outliers_prix.copy()
outliers_prix["multiplicateur_seuil"] = outliers_prix["old_price"] / outliers_prix["seuil_extreme"] # Mesure la gravité de l'écart (ex: un produit qui coûte 50 fois le maximum de son rayon).
top_outliers = outliers_prix.sort_values(by="multiplicateur_seuil", ascending=False).drop_duplicates(subset=["product_name"]).head(5) # Affiche les 5 pires cas distincts.

for _, ligne in top_outliers.iterrows():
    nom = str(ligne["product_name"])[:30]
    cat = str(ligne["category"])[:15]
    print(f"    * [{cat:<15}] {nom:<30} | Prix: {ligne['old_price']:>7.2f} € (Seuil max rayon: {ligne['seuil_extreme']:.2f} €)")
