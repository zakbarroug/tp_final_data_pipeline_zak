import streamlit as st
import pandas as pd

def data_prep(dfn: pd.DataFrame, dfi: pd.DataFrame) -> pd.DataFrame:
    """
    Cette fonction fusionne les deux DataFrames en utilisant l'ID de l'utilisateur
    et renvoie un nouveau DataFrame contenant les colonnes de dfn et dfi.
    Si les deux DataFrames ont une colonne en commun appelée "user_id",
    la fusion est effectuée directement. Sinon, la colonne "author_id" est renommée
    en "user_id" avant la fusion.
    """
  
    if "user_id" in dfn.columns:
        user_id_col = "user_id"
    else:
        user_id_col = "author_id"
        dfn = dfn.rename(columns={"author_id": "user_id"})


    required_cols = [user_id_col, "message_id"]
    for df in [dfn, dfi]:
        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"Les colonnes requises ne sont pas présentes dans le DataFrame: {df}")

    df_merged = pd.merge(dfn, dfi, on=user_id_col)

    
    output_file = "pipeline_result.csv"
    try:
        df_merged.to_csv(output_file, index=False)
    except OSError as e:
        st.error(f"Impossible d'enregistrer le résultat dans le fichier {output_file}: {e}")
        return None

    
    st.write(df_merged.head(10))

    return df_merged