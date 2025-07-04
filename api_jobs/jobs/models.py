from django.db import models

class JobOffer(models.Model):
    id = models.UUIDField(primary_key=True)
    job_title = models.TextField(null=True)
    normalized_title = models.TextField(null=True)
    company = models.TextField(null=True)
    location = models.TextField(null=True)
    source = models.TextField(null=True)
    country = models.TextField(null=True)
    date = models.DateField(null=True)
    description = models.TextField(null=True)
    skills = models.JSONField(null=True)
    salary_min = models.FloatField(null=True)
    salary_max = models.FloatField(null=True)
    currency = models.CharField(max_length=10, null=True)
    contract_type = models.CharField(max_length=50, null=True)

    class Meta:
        db_table = "job_offers"
        managed = False


class SalaryByCountry(models.Model):
    id = models.UUIDField(primary_key=True)
    normalized_title = models.TextField(null=True)
    country_name = models.TextField(null=True)
    salary_min = models.FloatField(null=True)
    salary_max = models.FloatField(null=True)
    salary_median = models.FloatField(null=True)
    date = models.DateField(null=True)
    currency = models.CharField(max_length=10, null=True)

    class Meta:
        db_table = "salary_by_country"
        managed = False


class RatingByCompany(models.Model):
    id = models.UUIDField(primary_key=True)
    company = models.TextField(null=True)
    rating = models.FloatField(null=True)

    class Meta:
        db_table = "rating_by_company"
        managed = False


class GoogleTrends(models.Model):
    id = models.UUIDField(primary_key=True)
    keyword = models.TextField(null=True)
    date = models.DateField(null=True)
    country = models.TextField(null=True)
    interest = models.FloatField(null=True)

    class Meta:
        db_table = "google_trends"
        managed = False


class GithubProjects(models.Model):
    id = models.UUIDField(primary_key=True)
    name = models.TextField(null=True)
    url = models.TextField(null=True)
    stars_total = models.IntegerField(null=True)
    stars_period = models.IntegerField(null=True)
    language = models.TextField(null=True)
    description = models.TextField(null=True)
    date = models.DateField(null=True)

    class Meta:
        db_table = "github_projects"
        managed = False
