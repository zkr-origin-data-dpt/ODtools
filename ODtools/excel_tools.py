import xlrd
import xlwt


def read_excel(filename: str = None, sheet: str = "Sheet1", read_by_row: bool = False):
    """
    读取excel文件
    :param filename: excel文件名
    :param sheet: excel表名
    :param read_by_row: 是否按行读取 默认按列读取
    :return:
    """
    assert filename, "No such file or directory: None"
    workbook = xlrd.open_workbook(filename)
    sheet = workbook.sheet_by_name(sheet)
    if read_by_row:
        return sheet.row_values
    return sheet.col_values


def write_excel(xls_name: str = "", sheet: str = "Sheet1", titles: list = None, lines: list = None):
    """
    写excel文件
    :param xls_name: excel名字
    :param sheet: 表名
    :param titles: 表头
    :param lines: 内容(二维数组)
    :return:
    """
    excel = xls_name + '.xls'
    f = xlwt.Workbook()
    sheet1 = f.add_sheet(sheet, cell_overwrite_ok=True)
    lines.insert(0, titles)
    for row in range(len(lines)):
        line_list = lines[row]
        for i in range(len(line_list)):
            sheet1.write(row, i, str(line_list[i]))
    f.save(excel)


if __name__ == '__main__':
    write_excel('test', titles=['num'], lines=[[1, [1, 1]], [2], [3]])
