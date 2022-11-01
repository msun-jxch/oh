import numpy as np
import pytesseract
from pdf2image import convert_from_path
import time
import threading


def image_ocr(text_arr, index, img):
    img = np.array(img)
    text_arr[index] = pytesseract.image_to_string(img, lang='chi_sim+eng')


def pdf_ocr(file_path, **kwargs):
    images = convert_from_path(file_path, **kwargs)
    images_cnt = len(images)
    threads = []
    text_arr = ['' for i in range(0, images_cnt + 1)]
    sum_time = 0
    start_time = time.time()
    for i, img in enumerate(images):
        print('\r', f'{i + 1} / {images_cnt}', end='', flush=True)
        task = threading.Thread(target=image_ocr, args=(text_arr, i, img))
        threads.append(task)
        task.start()

    for task in threads:
        task.join()

    sum_time += time.time() - start_time
    print(f'\n耗时: {sum_time} 秒')
    return ''.join(text_arr)


pdf_path = r'../res/GBZ 188-2014 职业健康监护技术规范.pdf'
txt_path = r'../res/GBZ 188-2014 职业健康监护技术规范.txt'
txt_context = pdf_ocr(pdf_path)

with open(txt_path, 'w', encoding='utf-8') as f:
    f.write(txt_context)

print('SUCCESS')
