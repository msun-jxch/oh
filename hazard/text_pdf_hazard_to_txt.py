from pdfminer.pdfinterp import PDFPageInterpreter, PDFResourceManager
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from pdfminer.pdfpage import PDFPage

pdf_file = open(r'../res/GBZ 188-2014 职业健康监护技术规范.pdf', 'rb')
txt_file = open(r'../res/GBZ 188-2014 职业健康监护技术规范.txt', 'a')

parser = PDFParser(pdf_file)
parser.set_document(doc=PDFDocument(parser=parser))
resource = PDFResourceManager()
device = PDFPageAggregator(resource, laparams=LAParams())
interpreter = PDFPageInterpreter(resource, device)

for page in PDFPage.get_pages(pdf_file):
    interpreter.process_page(page)
    layout = device.get_result()
    for out in layout:
        if hasattr(out, 'get_text'):
            print(out.get_text())
            txt_file.write(out.get_text())

txt_file.close()
print('SUCCESS')
