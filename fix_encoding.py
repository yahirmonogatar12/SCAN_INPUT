import re

file_path = 'app/ui/main_window.py'

with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
    content = f.read()

# Reemplazar caracteres mal codificados
replacements = {
    # Emojis y simbolos - eliminar
    'OK': 'OK',
    # Acentos
    'cache': 'cache',
    'linea': 'linea',
}

for old, new in replacements.items():
    content = content.replace(old, new)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Encoding fixed")
