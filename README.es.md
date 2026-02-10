# Daily Assignment - Telemarketing

[English](README.md) | **Español**

## Descripción
Sistema automatizado de asignación diaria de usuarios para operadores de telemarketing. El sistema distribuye usuarios de manera equitativa entre operadores considerando:

- **Distribución por campañas**: Cada operador puede manejar 1-3 campañas específicas
- **Balanceo por monedas**: Distribución inteligente según tipos de moneda (prioritarias, pequeñas, grandes, relevantes)
- **Exclusión de usuarios contactados**: Evita contactar usuarios recientemente contactados por telemarketing o email marketing según configuración
- **Algoritmo de asignación proporcional**: Distribución porcentual según número de campañas asignadas

## Estructura del Proyecto

```
daily_assignment_tlmkt/
├── data/                          # Archivos de salida de asignaciones (Excel)
├── src/
│   ├── config.py                  # Clase de configuración dinámica
│   ├── extract.py                 # Extracción de BigQuery y Google Sheets
│   ├── transform.py               # Algoritmos de asignación y normalización
│   ├── load.py                    # Carga de datos a BigQuery
│   └── __pycache__/              # Archivos compilados de Python
├── .env                          # Variables de entorno (no en repo)
├── .gitignore                    # Exclusiones de control de versiones
├── main.py                       # Ejecutable principal (compatible con Cloud Run)
├── test_main.py                  # Script de pruebas locales
├── requirements.txt              # Dependencias del proyecto
├── xxx-xxxxxx-xxxx-xxxx.json # Credenciales de servicio para BigQuery (no en repo)
├── xxxxxx-xxxxxxxxxxxx.json  # Credenciales de servicio para Sheets (no en repo)
├── README.md                     # Documentación del proyecto (inglés)
└── README.es.md                  # Documentación del proyecto (español)
```

## Configuración del Sistema

### Clase Config
La clase `Config` gestiona dinámicamente todos los parámetros del sistema desde Google Sheets:

```python
class Config:
    """
    Gestiona parámetros de configuración desde DataFrame.
    
    Convierte automáticamente tipos de datos:
    - int: Números enteros (días, cantidades)
    - float: Números decimales (porcentajes)  
    - str: Cadenas de texto
    - list(str): Listas de strings separadas por comas
    """
```

**Parámetros principales:**
- `days_ago_to_discard`: Días hacia atrás para excluir usuarios contactados por telemarketing o email marketing (ej: 7)
- `users_to_assign_per_operator`: Cantidad base de usuarios por operador (ej: 100)
- `currencies_to_filter`: Lista de monedas a excluir en la asignación (ej: ['USD', 'EUR', 'BRL'])
- `priority_currencies`: Monedas de alta prioridad para asignación temprana (ej: ['USD', 'EUR'])
- `max_priority_currencies_percent`: Porcentaje máximo de asignación para monedas prioritarias (ej: 0.4 = 40%)
- `small_currencies_to_limit`: Monedas pequeñas con porcentaje límite de asignación conjunto (ej: ['JPY', 'CAD'])
- `max_small_currencies_percent`: Porcentaje máximo total para monedas pequeñas (ej: 0.1 = 10%)
- `big_currencies_to_limit`: Monedas grandes a asignar con porcentaje límite dividido (ej: ['BRL', 'CLP'])
- `max_big_currencies_percent`: Porcentaje máximo de asignación para monedas grandes (ej: 0.3 = 30%)
- `relevant_currencies`: Monedas relevantes sin límite específico (ej: ['USD', 'EUR', 'BRL'])
- `extra_users_campaign`: Campañas adicionales para completar asignaciones (ej: ['non_depositors'])

### Sistema de Porcentajes por Campañas

El sistema utiliza un algoritmo de distribución proporcional basado en el número de campañas asignadas a cada operador:

```python
percentages = {
    1: [1.0],           # 100% para operadores con 1 campaña
    2: [0.7, 0.3],      # 70% y 30% para operadores con 2 campañas  
    3: [0.5, 0.3, 0.2]  # 50%, 30% y 20% para operadores con 3 campañas
}
```

**Lógica de asignación:**
- **1 campaña**: El operador recibe el 100% de sus usuarios asignados en esa campaña
- **2 campañas**: La campaña principal recibe 70%, la secundaria 30%
- **3 campañas**: Distribución 50%-30%-20% en orden de prioridad

**Ejemplo práctico:**
Si un operador debe recibir 100 usuarios y maneja 3 campañas:
- Campaña 1: 50 usuarios (50%)
- Campaña 2: 30 usuarios (30%)  
- Campaña 3: 20 usuarios (20%)

## Instalación y Configuración

### Prerrequisitos
```bash
# Python 3.8+
# Google Cloud CLI configurado
# Credenciales de BigQuery
```

### Instalación de dependencias
```bash
pip install -r requirements.txt
```

### Autenticación
```bash
# Usar credenciales CLI (recomendado para desarrollo)
gcloud auth application-default login

# O configurar variable de entorno para producción
export GOOGLE_APPLICATION_CREDENTIALS="ruta/a/service-account.json"
```

### Variables de Entorno para Cloud Run
Configura estas variables en la consola de Cloud Run:
- `SHEET_CREDENTIALS`: String JSON de las credenciales de la cuenta de servicio de Google Sheets

## Uso

### Pruebas Locales (test_main.py)
El script `test_main.py` te permite probar el código de `main.py` listo para Cloud Run localmente, simulando el entorno de Cloud Run:

```python
# test_main.py configura:
# 1. Credenciales de BigQuery: service-account.json
# 2. Credenciales de Sheets: variable de entorno SHEET_CREDENTIALS
# 3. Objeto request simulado para compatibilidad con Cloud Run

python test_main.py
```

**Propósito**: Probar el código exacto que se ejecutará en Cloud Run sin desplegarlo. Útil para:
- Validar la lógica de asignación antes del despliegue
- Depurar problemas de credenciales localmente
- Probar cambios de configuración
- Verificar extracción y transformación de datos

**Salida**: Log completo de ejecución con el estado del resultado final.

### Despliegue en Cloud Run
```bash
# Desplegar a Cloud Run
gcloud run deploy daily-assignment-tlmkt \
  --source . \
  --region southamerica-west1 \
  --allow-unauthenticated

# Configurar variable de entorno en la consola de Cloud Run:
# SHEET_CREDENTIALS = {contenido JSON de la cuenta de servicio de Google Sheets}
```

**Punto de entrada**: función `run_daily_assignment(request)` en main.py

## Mejoras Recientes (2026)

### Manejo Mejorado de Errores
- **Manejo de errores por tabla individual**: Si una tabla de BigQuery no existe o está vacía, el proceso continúa con las tablas restantes
- **Bloques try-except completos**: Todas las operaciones críticas envueltas con manejo de errores
- **Mensajes de error informativos**: Registro claro para depuración en Cloud Run

### Seguridad en Carga de Datos
- **Parámetro `delete_today`**: Control sobre si reemplazar o prevenir datos duplicados
  - `True`: Reemplaza los datos de hoy si existen
  - `False`: Previene la carga si ya existen datos para hoy (recomendado para producción)
- **Verificación inteligente de eliminación**: Verifica si existen datos antes de intentar eliminarlos
- **Mensajes de estado claros**: Los logs muestran exactamente qué pasó con los datos

### Características Adicionales
- **Columna `campaign_details`**: Soporte para metadatos de campañas externas
- **Seguimiento de descarga de tablas**: Muestra qué tablas se están descargando (📥 nombre_tabla)
- **Detección de tablas vacías**: Advierte cuando las tablas existen pero no contienen datos (⚠️)

## Flujo del Proceso

### 1. **Configuración y Credenciales**
- Carga configuración desde Google Sheets usando la clase `Config`
- Establece credenciales para BigQuery y Google Sheets
- Define fechas y parámetros de filtrado

### 2. **Extracción de Datos (Extract)**
- **Operadores activos**: Lista desde Google Sheet 'LP_TLMKT'
- **Usuarios disponibles**: Segmentos de BigQuery según configuración
- **Historial de asignaciones**: Usuarios contactados recientemente por telemarketing (`tlmkt_DailyAssignment`) y email marketing (`email_mkt_DailyAssignment`)
- **Configuración de campañas**: Parámetros dinámicos del sistema

### 3. **Transformación y Asignación (Transform)**
- **Filtrado de usuarios**: Exclusión de usuarios contactados recientemente por telemarketing o email marketing
- **Normalización de campañas**: Conversión entre códigos internos y nombres en español
- **Creación de DataFrames por campaña**: Organización de usuarios disponibles
- **Algoritmo de asignación en 4 fases**:
  1. **Monedas prioritarias** (con límite porcentual dividido)
  2. **Monedas pequeñas** (con límite porcentual total)
  3. **Monedas grandes** (con límite porcentual dividido)
  4. **Monedas relevantes** (sin límite, hasta completar)
- **Completación de asignaciones**: Uso de usuarios extra de otras campañas

### 4. **Carga de Datos (Load)**
- **Archivo local**: Excel con asignaciones del día
- **BigQuery**: Tabla de asignaciones históricas
- **Normalización final**: Conversión de códigos a nombres en español

## Archivos de Configuración

### Google Sheets requeridos:
1. **Daily_Assignment_Configuration** (Hoja 0): Parámetros del sistema
2. **Daily_Assignment_Configuration** (Hoja 1): Tablas de segmentos
3. **LP_TLMKT**: Lista de operadores activos

### Estructura de configuración:
| variable | value | type |
|----------|-------|------|
| days_ago_to_discard | 7 | int |
| users_to_assign_per_operator | 100 | int |
| priority_currencies | USD,EUR | list(str) |
| max_priority_currencies_percent | 0.4 | float |

## Mantenimiento y Administración

### Actualizar Operadores
Editar Google Sheet 'LP_TLMKT':
- **Nombre y Apellido**: Nombre completo del operador
- **Usuario DotPanel**: Username del sistema  
- **Campaña**: Lista de campañas separadas por comas
- **Cargo**: "Ejecutivo de Televentas"
- **Estatus**: "Activo" para incluir en asignaciones

### Modificar Parámetros del Sistema
Editar Google Sheet 'Daily_Assignment_Configuration':
- Cambiar porcentajes de monedas
- Ajustar días de exclusión
- Modificar cantidad de usuarios por operador
- Agregar nuevas monedas a listas

### Troubleshooting Común
- **Error de credenciales**: Verificar `gcloud auth list`
- **Datos faltantes**: Revisar Google Sheets de configuración
- **Asignaciones desbalanceadas**: Ajustar porcentajes en configuración
- **Tablas no encontradas**: Verificar nombres de tablas en BigQuery

## Ejemplos de Salida

### Logs de Ejecución
```
 tlmkt_Non_Depositors
 tlmkt_Second_Depositors
 tlmkt_Third_Depositors
⚠️  Table tlmkt_Active_Casino does not exist, skipping to next campaign
Data extracted successfully
Discarding users contacted since 2025-12-28
Available users for assignment: 15561
Creating assignment dictionary...
Assigning Priority Currencies...
Assigning Small Currencies...
Assigning Big Currencies...
Assigning Relevant Currencies...
User assignment process completed successfully.
Loading data to BigQuery...
Table mi-casino.dm_telemarketing.tlmkt_DailyAssignment has data for today. No new data will be appended.
Daily assignment process finalized successfully.
```

## Licencia
Este proyecto es de uso interno de la organización.
