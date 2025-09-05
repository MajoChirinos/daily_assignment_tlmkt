# Daily Assignment - Telemarketing

## Descripción
Sistema automatizado de asignación diaria de usuarios para operadores de telemarketing. El sistema distribuye usuarios de manera equitativa entre operadores considerando:

- **Distribución por campañas**: Cada operador puede manejar 1-3 campañas específicas
- **Balanceo por monedas**: Distribución inteligente según tipos de moneda (prioritarias, pequeñas, grandes, relevantes)
- **Exclusión de usuarios contactados**: Evita contactar usuarios recientes según configuración
- **Algoritmo de asignación proporcional**: Distribución porcentual según número de campañas asignadas

## Estructura del Proyecto

```
CloudRun_daily_assignment_tlmkt/
├── data/
│   ├── Telemarketing_Assignment_20250716.xlsx    # Archivo de asignación del 16/07/2025
│   ├── Telemarketing_Assignment_20250826.xlsx    # Archivo de asignación del 26/08/2025
│   └── Telemarketing_Assignment_20250827.xlsx    # Archivo de asignación del 27/08/2025
├── src/
│   ├── config.py                   # Clase de configuración dinámica
│   ├── extract.py                  # Extracción de BigQuery y Google Sheets
│   ├── transform.py                # Algoritmos de asignación y normalización
│   ├── load.py                     # Carga de datos a BigQuery
│   └── __pycache__/               # Archivos compilados de Python
├── .env                           # Variables de entorno (no incluir en repo)
├── .gitignore                     # Archivos excluidos del control de versiones
├── main.py                        # Script principal ejecutable
├── daily_assignment_v2.ipynb     # Notebook de desarrollo y análisis
├── requirements.txt               # Dependencias del proyecto
├── parabolic-water-352818-e036b2475893.json  # Credenciales de servicio (no incluir en repo)
└── README.md                      # Documentación del proyecto
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
- `days_ago_to_discard`: Días hacia atrás para excluir usuarios contactados (ej: 7)
- `users_to_assign_per_operator`: Cantidad base de usuarios por operador (ej: 100)
- `currencies_to_filter`: Lista de monedas a excluiren la asignación (ej: ['USD', 'EUR', 'BRL'])
- `priority_currencies`: Monedas de alta prioridad para asignación temprana (ej: ['USD', 'EUR'])
- `max_priority_currencies_percent`: Porcentaje máximo de asignación para monedas prioritarias (ej: 0.4 = 40%)
- `small_currencies_to_limit`: Monedas pequeñas con porcentaje límite de asiganción conjunto (ej: ['JPY', 'CAD'])
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
export GOOGLE_APPLICATION_CREDENTIALS="path/to/credentials.json"
```

## Uso del Sistema

### Ejecución local
```python
from main import run_daily_assignment

# Ejecutar asignación diaria
result = run_daily_assignment()
print(result)  # "Assignment Completed"
```

### Ejecución desde terminal
```bash
python main.py
```

## Flujo del Proceso

### 1. **Configuración y Credenciales**
- Carga configuración desde Google Sheets usando la clase `Config`
- Establece credenciales CLI para BigQuery y Google Sheets
- Define fechas y parámetros de filtrado

### 2. **Extracción de Datos (Extract)**
- **Operadores activos**: Lista desde Google Sheet 'LP_TLMKT'
- **Usuarios disponibles**: Segmentos de BigQuery según configuración
- **Historial de asignaciones**: Usuarios contactados recientemente
- **Configuración de campañas**: Parámetros dinámicos del sistema

### 3. **Transformación y Asignación (Transform)**
- **Filtrado de usuarios**: Exclusión de usuarios contactados recientemente
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

## Algoritmos de Asignación

### Distribución por Tipos de Moneda

1. **Priority Currencies** (`max_priority_currencies_percent`, `split_percentage=True`)
   - Límite porcentual dividido entre las monedas de la lista
   - Asignación circular equitativa entre operadores

2. **Small Currencies** (`max_small_currencies_percent`, `split_percentage=False`)  
   - Límite porcentual total para todas las monedas pequeñas combinadas
   - Distribución proporcional sin división por moneda

3. **Big Currencies** (`max_big_currencies_percent`, `split_percentage=True`)
   - Similar a priority currencies, límite dividido entre monedas
   - Asignación balanceada por operador

4. **Relevant Currencies** (sin límite)
   - Asignación hasta completar cuotas de operadores
   - Sin restricciones porcentuales

### Algoritmo de Completación
- **Prioridad 1**: Usuarios de la misma campaña con monedas prioritarias
- **Prioridad 2**: Usuarios de la misma campaña con monedas relevantes  
- **Prioridad 3**: Cualquier usuario de la misma campaña
- **Prioridad 4**: Usuarios de campañas extra con monedas prioritarias
- **Prioridad 5**: Usuarios de campañas extra con monedas relevantes
- **Prioridad 6**: Cualquier usuario de campañas extra

## Ejemplos de Salida

### Distribución por Moneda
| Moneda | Usuarios | Porcentaje |
|--------|----------|------------|
| USD    | 8,532    | 54.6%      |
| EUR    | 3,247    | 20.8%      |
| BRL    | 2,186    | 14.0%      |
| CLP    | 1,693    | 10.8%      |

### Asignación por Operador
| Operador | Campaña | Moneda | Usuarios Asignados |
|----------|---------|--------|--------------------|
| Ana García | No Depositantes | USD | 42 |
| Luis Pérez | Reactivación | CLP | 38 |
| María López | Segundo Depósito | BRL | 35 |

### Resumen de Asignación Final
```
Operador          Usuarios Asignados
Ana García                        95
Luis Pérez                        98  
María López                       97
Carlos Ruiz                      102
Total usuarios asignados:        392
```

## Monitoreo y Logs

El sistema genera logs detallados en tiempo real:

```
Extracting data to assign...
Data extracted successfully
Discarding users contacted since 2025-08-20
Available users for assignment: 15658

Creating assignment dictionary...
Assignment Dictionary created successfully.

Assigning Priority Currencies...
Assigning Small Currencies...
Assigning Big Currencies...  
Updating Assignment Dictionary...
Assigning Relevant Currencies...
Completing Assignment with Additional Users...
Assignment completed.

Saving assignment to local file...
Assignment saved to local file.
Loading data to BigQuery...
Data loaded to BigQuery successfully.
```

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

### Agregar Nuevas Campañas
1. Incluir tabla en hoja de configuración de segmentos
2. Actualizar normalización en `transform.py`:
   ```python
   pattern_map = {
       r'Nueva Campaña': 'new_campaign',
       # ... existing patterns
   }
   ```

### Troubleshooting Común
- **Error de credenciales**: Verificar `gcloud auth list`
- **Datos faltantes**: Revisar Google Sheets de configuración
- **Asignaciones desbalanceadas**: Ajustar porcentajes en configuración

