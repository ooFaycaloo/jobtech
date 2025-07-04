from rest_framework import serializers
from .models import (
    JobOffer,
    SalaryByCountry,
    RatingByCompany,
    GoogleTrends,
    GithubProjects,
)


class JobOfferSerializer(serializers.ModelSerializer):
    class Meta:
        model = JobOffer
        fields = '__all__'


class SalaryByCountrySerializer(serializers.ModelSerializer):
    class Meta:
        model = SalaryByCountry
        fields = '__all__'


class RatingByCompanySerializer(serializers.ModelSerializer):
    class Meta:
        model = RatingByCompany
        fields = '__all__'


class GoogleTrendsSerializer(serializers.ModelSerializer):
    class Meta:
        model = GoogleTrends
        fields = '__all__'


class GithubProjectsSerializer(serializers.ModelSerializer):
    class Meta:
        model = GithubProjects
        fields = '__all__'
