def crear_tablas():
    return """
    
        CREATE TABLE IF NOT EXISTS date_table(
            Date_key VARCHAR(150) PRIMARY KEY,
            Calendar_Month_Number INT,
            Calendar_Year INT
        );

        CREATE TABLE IF NOT EXISTS departamento(
            dpto_codigo INT PRIMARY KEY,
            dpto_nombre VARCHAR(150)
        );

        CREATE TABLE IF NOT EXISTS entidad(
            entidad_codigo INT PRIMARY KEY,
            entidad_nombre VARCHAR(150)
        );

        CREATE TABLE IF NOT EXISTS fact_indicador(
            ID SERIAL PRIMARY KEY,
            nombre VARCHAR(150),
            subcategoria VARCHAR(150),
            dato_numerico FLOAT8,
            dato_cualitativo VARCHAR(150),
            unidad VARCHAR(150),
            fuente VARCHAR(250),
            fecha_key VARCHAR(150) REFERENCES date_table (date_key),
            dpto_key INT REFERENCES departamento (dpto_codigo),
            entidad_key INT REFERENCES entidad (entidad_codigo)
        );

        DELETE FROM fact_indicador;
    """
