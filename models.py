

from tortoise import Tortoise, fields, run_async
from tortoise.models import Model


class Request(Model):
    id = fields.IntField(pk=True)
    cust_name = fields.TextField(null=True)
    dob = fields.DateField(null=True, )
    gender = fields.TextField(null=True)
    bvn = fields.BigIntField(null=True, unique=True)
    phone = fields.TextField(null=True)
    date_process = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "requests"

    def __str__(self):
        return self.cust_name + " " + self.dob + " " + self.gender + " " + self.bvn


class RequestTimeLog(Model):
    id = fields.IntField(pk=True)
    request: fields.ForeignKeyRelation[Request] = fields.ForeignKeyField(
        "models.Request", related_name="logs", null=True
    )
    date_time_start = fields.DatetimeField(auto_now_add=True)
    initial_response = fields.DatetimeField(null=True)
    final_request_sent = fields.DatetimeField(null=True)
    final_response = fields.DatetimeField(null=True)

    class Meta:
        table = "request_logs"

    def __str__(self):
        return f"{self.request} log"


class Ruid(Model):
    id = fields.IntField(pk=True)
    ruid = fields.BigIntField(null=True)
    # bvn = fields.BigIntField(null=True)
    bvn: fields.ForeignKeyRelation[Request] = fields.ForeignKeyField(
        "models.Request", related_name="ruids", null=True
    )
    date_process = fields.DatetimeField(auto_now_add=True)

    class Meta:
        table = "ruids"

    def __str__(self):
        return self.ruid


# class Ticket(Model):
#     id = fields.IntField(pk=True)
#     report_ticket = fields.TextField(null=True)
#     # bvn = fields.BigIntField(null=True)
#     # rqst: fields.ForeignKeyRelation[Request] = fields.ForeignKeyField(
#     #     "models.Request", related_name="rqst_tckts"
#     # )
#     ruid: fields.ForeignKeyRelation[Ruid] = fields.ForeignKeyField(
#         "models.Ruid", related_name="ruid_tckts"
#     )
#
#     class Meta:
#         table = "ruids"
#
#     def __str__(self):
#         return self.report_ticket


# sqlite:///data/db.sqlite
async def run():
    await Tortoise.init(db_url="sqlite://db.sqlite3", modules={"models": ["__main__"]})
    # await Tortoise.init(db_url="sqlite://:memory:", modules={"models": ["__main__"]})
    await Tortoise.generate_schemas()

    # cust = await Request.create(name="Test")
    # await Request.filter(id=cust.id).update(cust_name="Updated name")

    # print(await Request.filter(cust_name="Updated name").first())
    # >>> Updated name

    # await Request(name="Test 2").save()
    # print(await Request.all().values_list("id", flat=True))
    # >>> [1, 2]
    # print(await Request.all().values("id", "cust_name", "gender", "bvn"))
    # >>> [{'id': 1, 'name': 'Updated name'}, {'id': 2, 'name': 'Test 2'}]


if __name__ == "__main__":
    run_async(run())
