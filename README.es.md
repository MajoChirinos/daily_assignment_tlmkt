# Daily Assignment - Telemarketing

[English](README.md) | **Español**

## Descripción
Sistema automatizado de asignación diaria de usuarios para operadores de telemarketing. El proceso actual distribuye usuarios de manera equitativa por **país** y prioriza primero los usuarios de mayor prioridad dentro de cada país.

El sistema considera:

- **Distribución por país**: Cada operador recibe uno o más países desde `LP_TLMKT`
- **Orden por prioridad**: Los usuarios se asignan de mayor a menor prioridad (`ULTRA-1`, `ULTRA-2`, `ALTA-1`, ...)
- **Completar con fallback**: Países de respaldo opcionales pueden completar cuotas incompletas cuando un país se queda sin usuarios
- **Exclusión de usuarios contactados**: Excluye usuarios contactados desde `days_ago_to_discard` hacia ayer según configuración
- **Comportamiento controlado por configuración**: La asignación y el filtrado se controlan desde Google Sheets

## Referencias Operativas

- **Spreadsheet de configuración**: [Daily_Assignment_Configuration](https://docs.google.com/spreadsheets/d/1h7FemF3zjIMCjTwo4DPKrNa-5eE-seV5nAW6tNrdPhU/edit?usp=sharing)
  - Hoja 0 (parameters): configuración de asignación por campaña
  - Hoja 1 (segments_to_consult): segmentos a consultar
  - Hoja 2 (parameters_v2): configuración de asignación por país
- **Looker Studio**: [Dashboard de asignación de telemarketing](https://datastudio.google.com/reporting/08c9ba40-b514-4715-98d1-d8b22a7587a0/page/p_q0dsmy25zd/edit)
  - Página 1: asignación diaria por operador, ordenada por operador y prioridad para que al descargarse aparezcan primero los usuarios de mayor prioridad
  - Página 2: data diaria disponible por país y prioridad

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
    - bool: Valores booleanos (`TRUE` / `FALSE`, `1` / `0`, `yes` / `no`)
    """
```

**Parámetros principales:**
  - `days_ago_to_discard`: Días hacia atrás para excluir usuarios contactados por telemarketing o email marketing. La ventana de descarte va desde esta fecha hasta ayer.
  - `exclude_email_mkt_users`: Indica si se incluye el historial de email marketing en el descarte (`TRUE` / `FALSE`)
- `users_to_assign_per_operator`: Cantidad base de usuarios por operador (ej: 100)
  - `currencies_to_filter`: Lista de monedas a excluir en la extracción
  - `campaigns_to_filter`: Lista de campañas a excluir en la extracción
  - `extra_users_country`: Países de respaldo opcionales para completar cuotas incompletas

  ### Distribución por País del Operador

  El sistema utiliza un split proporcional basado en la cantidad de países asignados a cada operador:

```python
percentages = {
  1: [1.0],           # 100% para operadores con 1 país
  2: [0.7, 0.3],      # 70% y 30% para operadores con 2 países  
  3: [0.5, 0.3, 0.2]  # 50%, 30% y 20% para operadores con 3 países
}
```

**Lógica de asignación:**
- **1 país**: El operador recibe el 100% de su cuota en ese país
- **2 países**: El primer país recibe 70%, el segundo 30%
- **3 países**: Distribución 50%-30%-20% en el orden configurado

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
- **Creación de DataFrames por campaña**: Organización de usuarios disponibles para métricas y reportes
- **Asignación por cuota de país**: Los operadores reciben cuotas por país desde `LP_TLMKT`
- **Asignación 1 a 1**: Los usuarios se asignan uno por uno por operador para mantener equilibrio
- **Prioridad primero**: Los usuarios de mayor prioridad se asignan primero dentro de cada país
- **Países de fallback**: `extra_users_country` puede completar cuotas incompletas si está configurado

### 4. **Carga de Datos (Load)**
- **Archivo local**: Excel con asignaciones del día
- **BigQuery**: Tabla de asignaciones históricas
- **Normalización final**: Conversión de códigos a nombres en español

### Tablas de BigQuery

#### `tlmkt_DailyAssignment`

Esquema actual usado por el cargador:

| campo | tipo |
|-------|------|
| assignment_date | DATETIME |
| operator | STRING |
| campaign_name | STRING |
| user_id | INTEGER |
| username | STRING |
| firstLast_name | STRING |
| phone | STRING |
| level | INTEGER |
| register_currency | STRING |
| last_activity | DATETIME |
| campaign_details | STRING |
| priority | STRING |

#### `tlmkt_AssignmentMetrics`

Esquema actual usado por el cargador:

| campo | tipo |
|-------|------|
| assignment_date | DATETIME |
| country | STRING |
| priority | STRING |
| campaign | STRING |
| available_users | INTEGER |
| assigned_users | INTEGER |
| unassigned_users | INTEGER |

## Archivos de Configuración

### Google Sheets requeridos:
1. **Daily_Assignment_Configuration** (Hoja 0): Parámetros del sistema
2. **Daily_Assignment_Configuration** (Hoja 1): Tablas de segmentos
3. **Daily_Assignment_Configuration** (Hoja 2): Configuración de asignación por país
4. **LP_TLMKT**: Lista de operadores activos

### Estructura de configuración:
| variable | value | type |
|----------|-------|------|
| days_ago_to_discard | 7 | int |
| exclude_email_mkt_users | FALSE | bool |
| users_to_assign_per_operator | 100 | int |
| currencies_to_filter | BOB | list(str) |
| campaigns_to_filter | reactivation | list(str) |
| extra_users_country | VES | list(str) |

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
- Cambiar monedas y campañas excluidas
- Ajustar días de exclusión
- Modificar cantidad de usuarios por operador
- Activar o desactivar la exclusión de email marketing
- Definir países de fallback para completar cuotas incompletas

### Troubleshooting Común
- **Error de credenciales**: Verificar `gcloud auth list`
- **Datos faltantes**: Revisar Google Sheets de configuración
- **Asignaciones desbalanceadas**: Revisar el mapeo de países en LP, los países fallback y la distribución por prioridad
- **Desfase de esquema**: Verificar que las tablas de BQ incluyan `country` y `priority` en `tlmkt_AssignmentMetrics`
- **No carga en BQ**: Confirmar que `load_data=True` en `main.py`
- **Tablas no encontradas**: Verificar nombres de tablas en BigQuery

## Ejemplos de Salida

### Logs de Ejecución
```
 tlmkt_Non_Depositors
 tlmkt_Second_Depositors
 tlmkt_Third_Depositors
⚠️  Table tlmkt_Active_Casino does not exist, skipping to next campaign
Data extracted successfully
Discarding users contacted from 2025-12-28 to 2025-12-31
Available users for assignment: 15561
Available users by currency (after discarding contacted users):
  • VES: 9440 users
  • CLP: 1877 users
  • USD: 1699 users
Creating assignment dictionary...
Assigning users by country with global priority order...
User assignment process completed successfully.
Loading data to BigQuery...
Table mi-casino.dm_telemarketing.tlmkt_DailyAssignment has data for today. No new data will be appended.
Daily assignment process finalized successfully.
```

## Licencia
Este proyecto es de uso interno de la organización.
