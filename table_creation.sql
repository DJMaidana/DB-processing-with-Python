CREATE TABLE museos_cines_bibliotecas (
  Cod_Loc INTEGER,
  IdProvincia INTEGER,
  IdDepartamento INTEGER,
  Categoría TEXT,
  Provincia TEXT,
  Localidad TEXT,
  Nombre TEXT,
  Dirección TEXT,
  CP INTEGER,
  Teléfono INTEGER,  
  Mail TEXT,
  Web TEXT
);
CREATE TABLE registros (
  Nombre TEXT,
  Cantidad_total_de_registros INTEGER
);
CREATE TABLE cines (
  Provincia TEXT,
  Pantallas_total INTEGER,
  Butacas_total INTEGER,
  Espacios_INCAA INTEGER
)