from lxml import etree

class XMLSerializer:
    def __init__(self, output_path):
        self.output_path = output_path

    def serialize(self, data, root_element_name="SIAP"):
        root = etree.Element(root_element_name)
        
        # Exemplo simples de iteração sobre linhas de dados (assumindo lista de dicts ou tuples)
        # Ajustar conforme o formato real dos dados processados
        for row in data:
            item = etree.SubElement(root, "Item")
            for key, value in row.items():
                child = etree.SubElement(item, key)
                child.text = str(value)

        tree = etree.ElementTree(root)
        tree.write(self.output_path, pretty_print=True, xml_declaration=True, encoding="UTF-8")
        print(f"XML gerado em: {self.output_path}")
