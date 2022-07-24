from PyQt5.QtWidgets import QApplication, QWidget, QLabel
from PyQt5.QtGui import QPixmap
import sys

class AppHolaMundo(QWidget):
    def __init__(self):
        super(AppHolaMundo, self).__init__()
        self.emptywindow()
        
    def emptywindow(self):
        self.setGeometry(100,100,400,600)
        self.setWindowTitle("Mi primera ventana")
        self.displaylabels()
        
    def displaylabels(self):
        texto = QLabel(self)
        texto.setText("Hola")
        texto.move(500,15)
        
        image = r"señal.png"
        try:
            with open(image):
                labelimage = QLabel(self)
                pixmap = QPixmap(image)
                labelimage.setPixmap(pixmap)
                labelimage.move(30,30)
        except:
            print("No se encuentra")
      
#QWidget.setGeometry(100,100,400,600)  

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = AppHolaMundo()
    window.show()
    sys.exit(app.exec_())
        
