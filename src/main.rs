
use polars::prelude::*;
use std::{fs, io::Cursor};

//--------------------------------------------------------------------
//--------------------------------------------------------------------
//--------------------------------------------------------------------
//--------------------------------------------------------------------

fn main() -> PolarsResult<()> {
    let df_4305 = filtrar_diseno_4305()?;

    let df_4306 = filtrar_diseno_4306()?;

    println!("{}", df_4305.head(Some(5)));

    println!("{}", df_4305.tail(Some(5)));

    println!("{}", df_4306.head(Some(5)));

    println!("{}", df_4306.tail(Some(5)));

    Ok(())
}

//--------------------------------------------------------------------
//--------------------------------------------------------------------
//--------------------------------------------------------------------
//--------------------------------------------------------------------


fn importar_deudores() -> PolarsResult<DataFrame> {
    let ruta_func = "C:/Users/anima/Desktop";
    let fecha_func = "11.2025";


    let file_path = format!("{}/DEUDORES {}.txt", ruta_func, fecha_func);


    let bytes = match fs::read(&file_path) {
        Ok(b) => b,
        Err(e) => {
            return Err(
                PolarsError::ComputeError(
                    format!("No se pudo leer {file_path}: {e}").into()
                )
            );
        }
    };

    let texto_utf8: String = encoding_rs::mem::decode_latin1(&bytes).into_owned();


    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b';')
        .flexible(true)
        .from_reader(Cursor::new(texto_utf8));

    let mut c00: Vec<String> = Vec::new();
    let mut c01: Vec<String> = Vec::new();
    let mut c02: Vec<String> = Vec::new();
    let mut c03: Vec<String> = Vec::new();
    let mut c04: Vec<String> = Vec::new();
    let mut c05: Vec<String> = Vec::new();
    let mut c06: Vec<String> = Vec::new();
    let mut c07: Vec<String> = Vec::new();
    let mut c08: Vec<String> = Vec::new();
    let mut c09: Vec<String> = Vec::new();
    let mut c10: Vec<String> = Vec::new();
    let mut c11: Vec<String> = Vec::new();
    let mut c12: Vec<String> = Vec::new();
    let mut c13: Vec<String> = Vec::new();
    let mut c14: Vec<String> = Vec::new();
    let mut c15: Vec<String> = Vec::new();
    let mut c16: Vec<String> = Vec::new();
    let mut c17: Vec<String> = Vec::new();
    let mut c18: Vec<String> = Vec::new();
    let mut c19: Vec<String> = Vec::new();
    let mut c20: Vec<String> = Vec::new();
    let mut c21: Vec<String> = Vec::new();
    let mut c22: Vec<String> = Vec::new();
    let mut c23: Vec<String> = Vec::new();
    let mut c24: Vec<String> = Vec::new();
    let mut c25: Vec<String> = Vec::new();
    let mut c26: Vec<String> = Vec::new();
    let mut c27: Vec<String> = Vec::new();
    let mut c28: Vec<String> = Vec::new();


    for result in rdr.records() {
        let rec = match result {
            Ok(r) => r,
            Err(_) => continue,
        };

        // Para cada columna, preguntamos si existe
        c00.push(rec.get(0).unwrap_or("").trim().to_string());  
        c01.push(rec.get(1).unwrap_or("").trim().to_string());
        c02.push(rec.get(2).unwrap_or("").trim().to_string());
        c03.push(rec.get(3).unwrap_or("").trim().to_string());
        c04.push(rec.get(4).unwrap_or("").trim().to_string());
        c05.push(rec.get(5).unwrap_or("").trim().to_string());
        c06.push(rec.get(6).unwrap_or("").trim().to_string());
        c07.push(rec.get(7).unwrap_or("").trim().to_string());
        c08.push(rec.get(8).unwrap_or("").trim().to_string());
        c09.push(rec.get(9).unwrap_or("").trim().to_string());
        c10.push(rec.get(10).unwrap_or("").trim().to_string());
        c11.push(rec.get(11).unwrap_or("").trim().to_string());
        c12.push(rec.get(12).unwrap_or("").trim().to_string());
        c13.push(rec.get(13).unwrap_or("").trim().to_string());
        c14.push(rec.get(14).unwrap_or("").trim().to_string());
        c15.push(rec.get(15).unwrap_or("").trim().to_string());
        c16.push(rec.get(16).unwrap_or("").trim().to_string());
        c17.push(rec.get(17).unwrap_or("").trim().to_string());
        c18.push(rec.get(18).unwrap_or("").trim().to_string());
        c19.push(rec.get(19).unwrap_or("").trim().to_string());
        c20.push(rec.get(20).unwrap_or("").trim().to_string());
        c21.push(rec.get(21).unwrap_or("").trim().to_string());
        c22.push(rec.get(22).unwrap_or("").trim().to_string());
        c23.push(rec.get(23).unwrap_or("").trim().to_string());
        c24.push(rec.get(24).unwrap_or("").trim().to_string());
        c25.push(rec.get(25).unwrap_or("").trim().to_string());
        c26.push(rec.get(26).unwrap_or("").trim().to_string());
        c27.push(rec.get(27).unwrap_or("").trim().to_string());
        c28.push(rec.get(28).unwrap_or("").trim().to_string());

    }

    let df = DataFrame::new(vec![
        Series::new("00", c00),
        Series::new("01", c01),
        Series::new("02", c02),
        Series::new("03", c03),
        Series::new("04", c04),
        Series::new("05", c05),
        Series::new("06", c06),
        Series::new("07", c07),
        Series::new("08", c08),
        Series::new("09", c09),
        Series::new("10", c10),
        Series::new("11", c11),
        Series::new("12", c12),
        Series::new("13", c13),
        Series::new("14", c14),
        Series::new("15", c15),
        Series::new("16", c16),
        Series::new("17", c17),
        Series::new("18", c18),
        Series::new("19", c19),
        Series::new("20", c20),
        Series::new("21", c21),
        Series::new("22", c22),
        Series::new("23", c23),
        Series::new("24", c24),
        Series::new("25", c25),
        Series::new("26", c26),
        Series::new("27", c27),
        Series::new("28", c28),
    ])?;

    Ok(df)
}


//--------------------------------------------------------------------
//--------------------------------------------------------------------
//--------------------------------------------------------------------
//--------------------------------------------------------------------


fn filtrar_diseno_4305() -> PolarsResult<DataFrame> {

    let df_1 = importar_deudores()?;

    let columna_01 = df_1.column("01")?;
    let mascara = columna_01.equal("4305")?;
    let df_4 = df_1.filter(&mascara)?;

    let mut df_5 = df_4.drop_many(&["27", "28"]);

    df_5.rename("00", "diseno")?;
    df_5.rename("01", "tipo_ident")?;
    df_5.rename("02", "cuit")?;
    df_5.rename("03", "denominacion")?;
    df_5.rename("04", "categoria")?;
    df_5.rename("05", "residencia")?;
    df_5.rename("06", "gobierno")?;
    df_5.rename("07", "provincia")?;
    df_5.rename("08", "situacion")?;
    df_5.rename("09", "vinculacion")?;
    df_5.rename("10", "prev_asist_crediticia")?;
    df_5.rename("11", "prev_particip")?;
    df_5.rename("12", "prev_responsable")?;
    df_5.rename("13", "incremento_prev_general")?;
    df_5.rename("14", "incremento_prev_opcion")?;
    df_5.rename("15", "asistencia_vinculados")?;
    df_5.rename("16", "total_financiaciones")?;
    df_5.rename("17", "actividad_principal")?;
    df_5.rename("18", "informacion_transitoria")?;
    df_5.rename("19", "deudor_encuadrado")?;
    df_5.rename("20", "refinanciaciones")?;
    df_5.rename("21", "recateg_obligatoria")?;
    df_5.rename("22", "sit_juridica")?;
    df_5.rename("23", "irrecup_dispo_tecnica")?;
    df_5.rename("24", "dias_atraso")?;
    df_5.rename("25", "mipyme")?;
    df_5.rename("26", "sit_sin_reclasificacion")?;

    Ok(df_5)
}


//--------------------------------------------------------------------
//--------------------------------------------------------------------
//--------------------------------------------------------------------
//--------------------------------------------------------------------


fn filtrar_diseno_4306() -> PolarsResult<DataFrame> {

    let df_1 = importar_deudores()?;

    let columna_01 = df_1.column("01")?;
    let mascara = columna_01.equal("4306")?;
    let df_4 = df_1.filter(&mascara)?;

    let mut df_5 = df_4.drop_many(&["17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28"]);

    df_5.rename("00", "diseno")?;
    df_5.rename("01", "tipo_ident")?;
    df_5.rename("02", "cuit")?;
    df_5.rename("03", "tipo_asistencia")?;
    df_5.rename("04", "gtias_pref_a_capital_vencida")?;
    df_5.rename("05", "gtias_pref_a_capital_total")?;
    df_5.rename("06", "gtias_pref_b_capital_vencida")?;
    df_5.rename("07", "gtias_pref_b_capital_total")?;
    df_5.rename("08", "gtias_pref_b_intereses_vencida")?;
    df_5.rename("09", "gtias_pref_b_intereses_total")?;
    df_5.rename("10", "sin_gtias_pref_cap_vencida")?;
    df_5.rename("11", "sin_gtias_pref_cap_total")?;
    df_5.rename("12", "sin_gtias_pref_interes_vencida")?;
    df_5.rename("13", "sin_gtia_pref_interes_total")?;
    df_5.rename("14", "previsiones_minimas")?;
    df_5.rename("15", "fecha_refinanc")?;
    df_5.rename("16", "financiacion_mipyme")?;

    Ok(df_5)
}
