INSTRUCCIONES PARA ENRIQUECER CON CERTERO

1. Correr el script datos_enriquecimiento.py desde //marcos-rago@192.168.130.18/home/marcos-rago/Documents/IA/pruebas_desde_19_8_2021/
2. Una vez llega al correo los datos, descargarlas y colocarlas en la carpeta /home/tetotille/Escritorio/MOPC/enriquecimiento/
3. Ejecutar el archivo envio_enriquecimiento.py
4. Se genera un archivo en Datos_mandados con la fecha de hoy
5. Ese archivo se sube en la página de CERTERO https://www.certero.precorp.com.ar/
6. Una vez se procesa, se descarga el ZIP y se usa el archivo sumario.xlsx, lo transformamos a csv y lo guardamos en /home/tetotille/Escritorio/MOPC/enriquecimiento/
7. Ejecutamos mail_data_cleaning.py y phone_data_cleaning.py
8. Los datos para pasar a la base de datos se guardan en la carpeta "insiders"
