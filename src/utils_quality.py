# Calidad y métricas

Utilidades principales para:
- Calculadora de % nulos por campo
- Unicidad de claves (isbn13 o id canónico)
- Validación de fechas, idiomas y monedas
- Reporte quality_metrics.json para métricas SBDxx

## Ejemplo uso
```python
from utils_quality import validate_quality_metrics
validate_quality_metrics(df_libros)
```

## Referencias
Incluye ejemplos para aserciones bloqueantes, logs por regla, y trayectorias.