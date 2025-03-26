from django.db import models
from django.core.validators import MinValueValidator

# Model Khách hàng
class Customer(models.Model):
    SEGMENT_CHOICES = [
        ('A1', 'Nhân viên văn phòng, chủ doanh nghiệp (36-45 tuổi) có mức thu nhập cao'),
        ('A2', 'Nhân viên văn phòng, Freelancer ở Miền Bắc (25-35 tuổi)'),
        ('A3', 'Sinh viên, nhân viên văn phòng, freelancer (18-24 tuổi)'),
        ('B1', 'Người làm kinh doanh hoặc văn phòng (45+ tuổi) có thu nhập trung bình'),
        ('B2', 'Người làm nghề tự do hoặc nhân viên văn phòng (36-45 tuổi)'),
        ('B3', 'Người làm nghề tự do hoặc nhân viên văn phòng (25-35 tuổi) ở Miền Bắc'),
        ('C1', 'Sinh viên đi làm thêm, nhân viên mới đi làm (20-29 tuổi)'),
        ('C2', 'Nhân viên văn phòng, người làm việc tự do (30-45 tuổi) ở miền Bắc'),
        ('C3', 'Người mua để uống không có mục đích cụ thể (45+)'),
    ]
    
    customer_id = models.CharField(max_length=10, primary_key=True)  
    name = models.CharField(max_length=100, null=True, blank=True)  
    segment_code = models.CharField(max_length=3, choices=SEGMENT_CHOICES)  

    def __str__(self):
        return self.name or self.customer_id

    @property
    def segment_description(self):
        return dict(self.SEGMENT_CHOICES).get(self.segment_code, "Không xác định")

# Model Nhóm hàng
class ProductGroup(models.Model):
    GROUP_CHOICES = [
        ('BOT', 'Bột'),
        ('SET', 'Set trà'),
        ('THO', 'Trà hoa'),
        ('TTC', 'Trà củ, quả sấy'),
        ('TMX', 'Trà mix'),
    ]
    
    group_code = models.CharField(max_length=3, primary_key=True, choices=GROUP_CHOICES) 
    group_name = models.CharField(max_length=50, editable=False)  

    def save(self, *args, **kwargs):
        for code, name in self.GROUP_CHOICES:
            if code == self.group_code:
                self.group_name = name
                break
        super().save(*args, **kwargs)

    def __str__(self):
        return self.group_name

# Model Mặt hàng
class Product(models.Model):
    product_code = models.CharField(max_length=10, primary_key=True)
    name = models.CharField(max_length=100) 
    group = models.ForeignKey(ProductGroup, on_delete=models.CASCADE) 
    unit_price = models.IntegerField(validators=[MinValueValidator(0)]) 

    def __str__(self):
        return self.name

# Model Đơn hàng
class Order(models.Model):
    order_id = models.CharField(max_length=10, primary_key=True, unique=True, editable=False)  
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE) 
    order_time = models.DateTimeField() 

    def save(self, *args, **kwargs):
        if not self.order_id:
            last_order = Order.objects.order_by('-order_id').first()
            if last_order:
                last_id = int(last_order.order_id.replace('DH', ''))
                new_id = last_id + 1
            else:
                new_id = 1
            self.order_id = f"DH{new_id:04d}"
        super().save(*args, **kwargs)

    def __str__(self):
        return self.order_id

# Model Chi tiết đơn hàng
class OrderDetail(models.Model):
    order = models.ForeignKey(Order, on_delete=models.CASCADE) 
    product = models.ForeignKey(Product, on_delete=models.CASCADE) 
    quantity = models.IntegerField(validators=[MinValueValidator(1)])  

    def __str__(self):
        return f"{self.order.order_id} - {self.product.name}"

    @property
    def total_price(self):
        return self.quantity * self.product.unit_price

    class Meta:
        unique_together = ('order', 'product') 
