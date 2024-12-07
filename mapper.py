#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!/usr/bin/env python
import sys
import csv

# Lire depuis l'entrée standard (stdin)
for line in sys.stdin:
    line = line.strip()
    fields = line.split(',')
    
    # Ignorer l'en-tête
    if fields[0] == "Order ID":
        continue
    
    product = fields[1]  # Nom du produit
    quantity_ordered = float(fields[2])  # Quantité commandée
    price_each = float(fields[3])  # Prix de chaque unité
    
    # Calcul de la valeur totale de la commande
    total_value = quantity_ordered * price_each
    
    # Émettre la paire clé-valeur (produit, valeur totale)
    print(f"{product}\t{total_value}")


# In[ ]:




