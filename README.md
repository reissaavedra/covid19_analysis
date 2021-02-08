# covid19_analysis_platzi
#  Análisis de Covid 

Este repositorio analiza información de distintos orígenes de datos.

*covid19_coaid.bak: *  Backup de la base de datos a utilizar en Postgresql 12.
Esta base de datos contiene información sobre distintos tweets clasificados como falsos.
*covid_tracking.bak: *  Backup de la base de datos a utilizar en Postgresql 12, conteniendo información relevante a la propagación del virus en USA y sus estados, así como información sobre los test realizados.

## Analysis Time Series
Aquí encontraremos 4 notebooks muy relevantes.
* misleading graphics: análisis respecto a algunos gráficos del que hacen uso algunos medios que podrían proporcionar una mejor visión.

* collect_data_covid_tracking: nos permite extraer información de la API de Covid Tracking para almacenarla en una database local y poder analizarla.

* covid_analysis_time_series: Hace uso de la información recolectada para poder plantear y realizar análisis estaísticos.

* Rt_introduction | Rt_aplication: Se realiza un análisis bayesiano a la cantidad de personas que se infectan por persona infectada.
Rt_introduction presenta una visión general con algunas simulaciones y explicaciones del método a realizar.
Rt_aplication aplica la metodología a la información recolectada.

## Analysis Twitter

* collect_data_ifcn: Se utilizan técnicas de webscrapping dinámica empleando Selenium para poder extraer la data almacenada en la página web y almacenarla localmente, permitiéndonos enriquecer nuestro dataset.

* collect_data_twitter: Este notebook nos permite obtener la data almacenada en el siguiente repo: https://github.com/cuilimeng/CoAID y almacenarla en una base de datos local. Posteriormente los tweets son hidratados mediante la API de Twitter.

*analysis_data_twitter: Procesos de limpieza y visualización de la data recolectada mediante los notebooks anteriores.

## License
[MIT](https://choosealicense.com/licenses/mit/)
