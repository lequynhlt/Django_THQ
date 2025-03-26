# d3app/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('', views.data_visualization, name='data_visualization'),  # Trang chính hiển thị biểu đồ
    path('import/', views.import_csv, name='import_csv'),  # Trang nhập dữ liệu CSV
]