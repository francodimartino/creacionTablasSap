from helpers import *

def main():
    conexion=conectar_db("localhost", "5433", "creaciontablas", "postgres", "postgres")
    zf01(conexion)
    conexion.commit()
    zco_covp(conexion)
    conexion.commit()
    conexion.close()

def zf01(conexion):

    rutaZF01="//192.168.100.55/Reportes/ReportesSP/Polakof y Cia S.A/ReportesSP - Documentos/01. ExtractorER/01. EJ_VIG/F01/PPTO_VIGdet.txt"
    crear_o_truncar_tabla(rutaZF01, conexion,  "zf01", True)
    cargar_carpeta_en_tabla(conexion, "//192.168.100.55/Reportes/ReportesSP/Polakof y Cia S.A/ReportesSP - Documentos/01. ExtractorER/01. EJ_VIG/F01/", "zf01")

def zco_covp(conexion):
    
    rutaZCO_COVP="//192.168.100.55/Reportes/ReportesSP/Polakof y Cia S.A/ReportesSP - Documentos/01. ExtractorER/01. EJ_VIG/01.ERsin2/ERsin2_01_07_01.txt"
    crear_o_truncar_tabla(rutaZCO_COVP, conexion,  "zco_covp")
    conexion.commit()
    ruta_base="//192.168.100.55/Reportes/ReportesSP/Polakof y Cia S.A/ReportesSP - Documentos/01. ExtractorER/"
    subcarpetas=["01. EJ_VIG/01.ERsin2/", "01. EJ_VIG/02.VTA/", "02. EJ_ANT/01.ERsin2/", "02. EJ_ANT/02.VTA/"]
    for subcarpeta in subcarpetas:
        cargar_carpeta_en_tabla(conexion, ruta_base+subcarpeta, "zco_covp")
        conexion.commit()
    
if __name__ == "__main__":
    main()