import xlsxwriter

# Create an new Excel file and add a worksheet.
workbook = xlsxwriter.Workbook('images.xlsx')
worksheet = workbook.add_worksheet()
image_width = 140.0
image_height = 182.0
cell_width = 64.0
cell_height = 20.0
x_scale = cell_width/image_width
y_scale = cell_height/image_height
worksheet.insert_image('B2', 'señal.png',{'x_scale': x_scale, 'y_scale': y_scale})

workbook.close()