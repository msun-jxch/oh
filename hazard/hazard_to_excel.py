import re
import copy
import collections
from pandas.core.frame import DataFrame
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG)

excel_path = r'../res/危害因素关联.xlsx'
f = open(r'../res/危害因素关联.txt', 'r', encoding='utf-8')
codes = range(5, 10)

lines = copy.deepcopy(f.readlines())


def is_sub_title(title, level1s):
    for k, v in level1s.items():
        if re.search(f'^{k}\\.', title.strip()) is not None:
            return True
    return False


def title_code(title):
    return ''.join(re.findall(r"^[0-9\\.]*", title.strip())).strip()


def father_title_code(this_code):
    sub_codes = this_code.split('.')
    sub_codes.pop()
    return '.'.join(sub_codes)


def title_name(title):
    return title.strip().replace(title_code(title), '').strip()


def title_level(title):
    return len(title_code(title).split('.'))


def is_level1(title):
    return title_level(title) == 1 and re.search(f'^[5-9][^).1-9，）]', title) is not None


def is_level2(title):
    return title_level(title) == 2


def is_level3(title):
    return title_level(title) == 3


def is_level4(title):
    return title_level(title) == 4


def get_sheet_name(title, level1s):
    for k, v in level1s.items():
        if re.search(f'^{k}\\.', title.strip()) is not None:
            return v
    raise Exception(f"没有对应的 title:{title}. level1s:{level1s}")


def build_level1_names():
    sheets = {}
    for row in lines:
        if is_level1(row):
            sheets[row[:1]] = row.replace('\n', '')
    return sheets


def is_alias_text(text_str):
    return re.search(f'同[0-9.]*', text_str) is not None


def alias_text_code(text_str):
    return ''.join(re.findall(f'同[0-9.]*', text_str)).strip('同').strip()


def trim(match_context):
    # match_context = match_context.strip().strip('。').strip().strip('.').strip().strip(',') \
    #     .strip().strip('，').strip().strip('；').strip().strip(';').strip()
    # match_context = re.sub(r'[a-z][)）]', "", match_context.strip())
    # match_context = re.sub(r'[；;]', ",", match_context.strip())
    # match_context = re.sub(r'[0-9 ]*[)）] ', "", match_context.strip())
    return match_context.strip().rstrip('b)').rstrip('c)').strip()


def has_zyb(match_context):
    return '职业病：' in match_context


def get_zyb(match_context):
    return trim(match_context.split('职业病：')[1].split('职业禁忌证：')[0])


def has_zyjjz(match_context):
    return '职业禁忌证：' in match_context


def get_zyjjz(match_context):
    return trim(match_context.split('职业禁忌证：')[1])


def has_zzxw(match_context):
    return '症状询问：' in match_context


def get_zzxw(match_context):
    return trim(match_context.split('症状询问：')[1].split('体格检查：')[0].split('实验室和其他检查：')[0])


def has_tgjc(match_context):
    return '体格检查：' in match_context


def get_tgjc(match_context):
    return trim(match_context.split('体格检查：')[1].split('实验室和其他检查：')[0])


def has_sysjc(match_context):
    return '实验室和其他检查：' in match_context


def get_sysjc(match_context):
    return trim(match_context.split('实验室和其他检查：')[1])


def has_bjxm(match_context):
    return '必检项目：' in match_context


def get_bjxm(match_context):
    return trim(match_context.split('必检项目：')[1].split('选检项目：')[0])


def has_xjxm(match_context):
    return '选检项目：' in match_context


def get_xjxm(match_context):
    return trim(match_context.split('选检项目：')[1])


def is_mbjb(match_context):
    return '目标疾病' in match_context


def is_jcnr(match_context):
    return '检查内容' in match_context


def is_jkjczq(match_context):
    return '健康检查周期' in match_context


def is_jcsj(match_context):
    return '检查时间' in match_context


def is_jcdx(match_context):
    return '检查对象' in match_context


def get_tgjc_tjxm_txt_arr(match_context):
    return [re.sub(r'[0-9][)）]', '', txt.strip('。')).strip()
            if '；' in match_context else txt.strip('。')
            for txt in match_context.split("；")]


def get_tgjc_tjxm_arr(match_context):
    return [re.sub(r'[0-9][)）]', '', txt.strip('。')).split('：')[0].strip()
            if '；' in match_context else txt.strip('。')
            for txt in match_context.split("；")]


def get_tgjc_jcxm(tjxm_match_context):
    return tjxm_match_context.split('：')[1].strip() if '：' in tjxm_match_context else ''


def get_tgjc_bjxm_txt_arr(bjxm_match_context):
    return bjxm_match_context.strip().strip('2').strip('。').strip('；').strip(';').split('、')


def get_tgjc_xjxm_txt_arr(xjxm_match_context):
    return xjxm_match_context.strip().strip('2').strip('。').strip('；').strip(';').split('、')


def get_mbjb_zyjjz_txt_arr(zyjjz_match_context):
    return [re.sub(r'[a-z0-9][)）]', '', txt.strip('。')).split('：')[0].strip()
            if '；' in zyjjz_match_context else txt.strip('。')
            for txt in zyjjz_match_context.split("；")]


def get_mbjb_zyb_txt_arr(zyjjz_match_context):
    return [txt.lstrip('1)').lstrip('2)').lstrip('3)').lstrip('4)').lstrip('5)')
            .lstrip('1）').lstrip('2）').lstrip('3）').lstrip('4）').lstrip('5）')
            .lstrip('a）').lstrip('b）').lstrip('c）').lstrip('d）').lstrip('e）')
            .lstrip('a)').lstrip('b)').lstrip('c)').lstrip('d)').lstrip('e)')
            .strip('。')
            .strip()
            if '；' in zyjjz_match_context else txt.strip('。')
            for txt in zyjjz_match_context.split("；")]


def append_base_vs_obj(base_hazard_vs_mbjb, base_key, base_item):
    # noinspection PyBroadException
    try:
        base_code = base_item['title_code']
        base_hazard_vs_mbjb[base_key] = {
            'title_code': base_code,
            'title_name': base_item['title_name'],
            '危害因素': title_context[father_title_code(father_title_code(base_code))]['title_name'],
            '岗位状态': title_context[father_title_code(base_code)]['title_name'],
            'context': base_item['context'],
        }
        return base_hazard_vs_mbjb
    except Exception:
        logging.error(f'base_key:{base_key}, base_item:{base_item}')


level1_names = build_level1_names()
title_context = collections.OrderedDict()

for line in lines:
    text = line.strip()
    if is_level1(text):
        code = text[:1]
        title_context[code] = {'title_code': code, 'title_name': text[1:], 'context': ''}
    if is_sub_title(text, level1_names):
        code = title_code(text)
        name = title_name(text)
        context = '：'.join(name.split('：')[1:]) if not is_level2(text) and len(name.split('：')) > 1 else ''
        name = name.split('：')[0] if not is_level2(text) else name
        title_context[code] = {'title_code': code, 'title_name': name, 'context': context}
    elif not is_level1(text):
        key = next(reversed(title_context))
        title_context[key]['context'] = title_context[key]['context'] + text

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='w') as writer:
    DataFrame(title_context.values()).to_excel(writer, '目录', index=False, header=True)
    logging.info(f'保存目录结构成功')

hazard_vs_mbjb = collections.OrderedDict()
for key, item in title_context.items():
    context = item['context']
    if is_mbjb(item['title_name']):
        append_base_vs_obj(hazard_vs_mbjb, key, item)
        if has_zyb(context):
            hazard_vs_mbjb[key]['职业病'] = get_zyb(context)
        if has_zyjjz(context):
            hazard_vs_mbjb[key]['职业禁忌症'] = get_zyjjz(context)
        if not has_zyb(context) and not has_zyjjz(context):
            hazard_vs_mbjb[key]['职业病'] = context

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a') as writer:
    DataFrame(hazard_vs_mbjb.values()).to_excel(writer, '目标疾病', index=False, header=True)
    logging.info(f'保存目标疾成功')

hazard_vs_jcnr = collections.OrderedDict()
for key, item in title_context.items():
    context = item['context']
    if is_jcnr(item['title_name']):
        append_base_vs_obj(hazard_vs_jcnr, key, item)
        if has_zzxw(context):
            hazard_vs_jcnr[key]['症状询问'] = get_zzxw(context)
        if has_tgjc(context):
            hazard_vs_jcnr[key]['体格检查'] = get_tgjc(context)
        if has_sysjc(context):
            hazard_vs_jcnr[key]['实验室和其他检查'] = get_sysjc(context)
        if has_bjxm(context):
            hazard_vs_jcnr[key]['必检项目'] = get_bjxm(context)
        if has_xjxm(context):
            hazard_vs_jcnr[key]['选检项目'] = get_xjxm(context)

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a') as writer:
    DataFrame(hazard_vs_jcnr.values()).to_excel(writer, '检查内容', index=False, header=True)
    logging.info(f'保存检查内容成功')

hazard_vs_jkjczq = collections.OrderedDict()
for key, item in title_context.items():
    if is_jkjczq(item['title_name']):
        append_base_vs_obj(hazard_vs_jkjczq, key, item)

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a') as writer:
    DataFrame(hazard_vs_jkjczq.values()).to_excel(writer, '健康检查周期', index=False, header=True)
    logging.info(f'保存健康检查周期成功')

hazard_vs_jcsj = collections.OrderedDict()
for key, item in title_context.items():
    if is_jcsj(item['title_name']):
        append_base_vs_obj(hazard_vs_jcsj, key, item)

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a') as writer:
    DataFrame(hazard_vs_jcsj.values()).to_excel(writer, '检查时间', index=False, header=True)
    logging.info(f'保存检查时间成功')

hazard_vs_jcdx = collections.OrderedDict()
for key, item in title_context.items():
    if is_jcdx(item['title_name']):
        append_base_vs_obj(hazard_vs_jcdx, key, item)

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a') as writer:
    DataFrame(hazard_vs_jcdx.values()).to_excel(writer, '检查对象', index=False, header=True)
    logging.info(f'保存检查对象成功')

hazard_vs_tgjc = []
for key, item in hazard_vs_jcnr.items():
    context = item['context']
    tgjc_base = append_base_vs_obj({}, key, item)[key]
    if '体格检查' in item:
        for tjxm in get_tgjc_tjxm_txt_arr(item['体格检查']):
            tgjc = copy.deepcopy(tgjc_base)
            tgjc['体格检查'] = item['体格检查']
            tgjc['体检项目'] = tjxm.split('：')[0].strip()
            tgjc['检查项目'] = get_tgjc_jcxm(tjxm)
            if tgjc['体检项目'] != '':
                hazard_vs_tgjc.append(tgjc)

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a') as writer:
    DataFrame(hazard_vs_tgjc).to_excel(writer, '体格检查', index=False, header=True)
    logging.info(f'保存体格检查成功')

hazard_vs_bjxm = []
for key, item in hazard_vs_jcnr.items():
    context = item['context']
    bjxm_base = append_base_vs_obj({}, key, item)[key]
    if '必检项目' in item:
        for bjxm_text in get_tgjc_bjxm_txt_arr(item['必检项目']):
            bjxm = copy.deepcopy(bjxm_base)
            bjxm['所有必检项目'] = item['必检项目']
            bjxm['必检项目'] = bjxm_text
            if bjxm['必检项目'] != '':
                hazard_vs_bjxm.append(bjxm)

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a') as writer:
    DataFrame(hazard_vs_bjxm).to_excel(writer, '必检项目', index=False, header=True)
    logging.info(f'保存必检项目成功')

hazard_vs_xjxm = []
for key, item in hazard_vs_jcnr.items():
    context = item['context']
    xjxm_base = append_base_vs_obj({}, key, item)[key]
    if '选检项目' in item:
        for xjxm_text in get_tgjc_xjxm_txt_arr(item['选检项目']):
            xjxm = copy.deepcopy(xjxm_base)
            xjxm['所有选检项目'] = item['选检项目']
            xjxm['选检项目'] = xjxm_text
            if xjxm['选检项目'] != '':
                hazard_vs_xjxm.append(xjxm)

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a') as writer:
    DataFrame(hazard_vs_xjxm).to_excel(writer, '选检项目', index=False, header=True)
    logging.info(f'保存选检项目成功')

hazard_vs_zyjjz = []
for key, item in hazard_vs_mbjb.items():
    context = item['context']
    zyjjz_base = append_base_vs_obj({}, key, item)[key]
    if '职业禁忌症' in item:
        for zyjjz_text in get_mbjb_zyjjz_txt_arr(item['职业禁忌症']):
            zyjjz = copy.deepcopy(zyjjz_base)
            zyjjz['所有职业禁忌症'] = item['职业禁忌症']
            zyjjz['职业禁忌症'] = zyjjz_text
            if zyjjz['职业禁忌症'] != '':
                hazard_vs_zyjjz.append(zyjjz)

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a') as writer:
    DataFrame(hazard_vs_zyjjz).to_excel(writer, '职业禁忌症', index=False, header=True)
    logging.info(f'保存职业禁忌症成功')

hazard_vs_zyb = []
for key, item in hazard_vs_mbjb.items():
    context = item['context']
    zyb_base = append_base_vs_obj({}, key, item)[key]
    if '职业病' in item:
        for zyb_text in get_mbjb_zyb_txt_arr(item['职业病']):
            zyb = copy.deepcopy(zyb_base)
            zyb['所有职业病'] = item['职业病']
            zyb['职业病'] = zyb_text
            if zyb['职业病'] != '':
                hazard_vs_zyb.append(zyb)

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a') as writer:
    DataFrame(hazard_vs_zyb).to_excel(writer, '职业病', index=False, header=True)
    logging.info(f'保存职业病成功')

all_zyb = list(
    map(lambda x: {'职业病': x['职业病'].replace('(', '（').replace(')', '）').replace(' ', '')}, hazard_vs_zyb))

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a') as writer:
    DataFrame(all_zyb).to_excel(writer, '职业病列表', index=False, header=True)
    logging.info(f'保存职业病列表成功')

all_zyjjz = list(
    map(lambda x: {'职业禁忌症': x['职业禁忌症'].replace('(', '（').replace(')', '）').replace(' ', '')},
        hazard_vs_zyjjz))

with pd.ExcelWriter(excel_path, engine='openpyxl', mode='a') as writer:
    DataFrame(all_zyjjz).to_excel(writer, '职业禁忌症列表', index=False, header=True)
    logging.info(f'保存职业禁忌症列表成功')
