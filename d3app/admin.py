from django.contrib import admin

from .models import Customer, ProductGroup, Product, Order, OrderDetail

admin.site.register(Customer)
admin.site.register(ProductGroup)
admin.site.register(Product)
admin.site.register(Order)
admin.site.register(OrderDetail)
