from faker import Faker


def generate_record():
    fake = Faker()
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone_number = fake.phone_number()[:10]
    address = fake.address()[:10]
    values = (first_name, last_name, email, phone_number, address)
    return values
