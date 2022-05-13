CREATE TABLE museos_cines_bibliotecas (
  cod_localidad INTEGER,
  id_provincia INTEGER,
  id_departamento INTEGER,
  categoría TEXT,
  provincia TEXT,
  localidad TEXT,
  nombre TEXT,
  domicilio TEXT,
  codigo_postal INTEGER,
  telefono INTEGER,  
  mail TEXT,
  web TEXT
);
CREATE TABLE registros (
  registros_por_categoria INTEGER,
  registros_por_fuente INTEGER,
  registros_por_provincia INTEGER
);
CREATE TABLE cines (
  provincia TEXT,
  pantallas INTEGER,
  butacas INTEGER,
  espacios_incaa INTEGER
)