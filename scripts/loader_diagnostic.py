from gastrometric.knowledge.loader import knowledge

print("Testing 'rib':", knowledge.relationships_for_subject("vocabulary", "rib"))
print("Testing 'ribs':", knowledge.relationships_for_subject("vocabulary", "ribs"))
print("Testing 'pork ribs':", knowledge.relationships_for_subject("vocabulary", "pork ribs"))