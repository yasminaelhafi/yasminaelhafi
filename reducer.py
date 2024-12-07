#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
import sys

current_product = None
total_sales = 0

# Lire depuis l'entrée standard (stdin)
for line in sys.stdin:
    line = line.strip()
    product, value = line.split('\t')
    value = float(value)  # Convertir la valeur en nombre à virgule flottante
    
    # Si nous avons changé de produit, afficher le total pour le produit précédent
    if current_product and current_product != product:
        print(f"{current_product}\t{total_sales}")
        total_sales = 0
    
    current_product = product
    total_sales += value

# Afficher le total pour le dernier produit
if current_product:
    print(f"{current_product}\t{total_sales}")


# In[ ]:




