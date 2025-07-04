from rest_framework import viewsets, status, filters
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated, AllowAny
from rest_framework.decorators import api_view, permission_classes
from django.contrib.auth.models import User
from rest_framework_simplejwt.tokens import RefreshToken
from django_filters.rest_framework import DjangoFilterBackend
from django.db.models import Avg, Min, Max, Count

from .models import *
from .serializers import *

class RegisterView(APIView):
    permission_classes = [AllowAny]

    def post(self, request):
        username = request.data.get("username")
        password = request.data.get("password")

        if User.objects.filter(username=username).exists():
            return Response({"error": "Username already exists"}, status=400)

        user = User.objects.create_user(username=username, password=password)
        refresh = RefreshToken.for_user(user)

        return Response({
            "message": "User created",
            "access": str(refresh.access_token),
            "refresh": str(refresh),
        }, status=201)

class JobOfferViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = JobOffer.objects.all()
    serializer_class = JobOfferSerializer
    permission_classes = [IsAuthenticated]
    filter_backends = [filters.SearchFilter, DjangoFilterBackend]
    search_fields = ['job_title', 'location', 'skills']


class SalaryByCountryViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = SalaryByCountry.objects.all()
    serializer_class = SalaryByCountrySerializer
    permission_classes = [IsAuthenticated]


class RatingByCompanyViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = RatingByCompany.objects.all()
    serializer_class = RatingByCompanySerializer
    permission_classes = [IsAuthenticated]


class GoogleTrendsViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = GoogleTrends.objects.all()
    serializer_class = GoogleTrendsSerializer
    permission_classes = [IsAuthenticated]


class GithubProjectsViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = GithubProjects.objects.all()
    serializer_class = GithubProjectsSerializer
    permission_classes = [IsAuthenticated]


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def salary_stats(request):
    title = request.GET.get('title')
    country = request.GET.get('country')
    if not title or not country:
        return Response({"error": "Missing parameters"}, status=400)
    data = SalaryByCountry.objects.filter(normalized_title__iexact=title, country_name__iexact=country)
    stats = data.aggregate(salary_min=Min('salary_min'), salary_max=Max('salary_max'), salary_median=Avg('salary_median'))
    return Response(stats)

@api_view(['GET'])
@permission_classes([IsAuthenticated])
def top_jobs_by_salary(request):
    country = request.GET.get('country')
    if not country:
        return Response({"error": "Missing country"}, status=400)
    try:
        result = (
            SalaryByCountry.objects.filter(country_name__iexact=country)
            .values('normalized_title')
            .annotate(avg_salary=Avg('salary_median'))
            .order_by('-avg_salary')[:5]
        )
        return Response(result)
    except Exception as e:
        return Response({"error": str(e)}, status=500)



@api_view(['GET'])
@permission_classes([IsAuthenticated])
def top_country_for_job(request):
    title = request.GET.get('title')

    if title:
        result = (
            SalaryByCountry.objects.filter(normalized_title__iexact=title)
            .values('country_name')
            .annotate(avg_salary=Avg('salary_median'))
            .order_by('-avg_salary')
        )
    else:
        result = (
            SalaryByCountry.objects
            .values('country_name')
            .annotate(avg_salary=Avg('salary_median'))
            .order_by('-avg_salary')
        )

    return Response(result)



@api_view(['GET'])
@permission_classes([IsAuthenticated])
def salary_trend_over_time(request):
    title = request.GET.get('title')
    if not title:
        return Response({"error": "Missing title"}, status=400)
    trend = (
        SalaryByCountry.objects.filter(normalized_title__iexact=title)
        .values('date')
        .annotate(avg_salary=Avg('salary_median'))
        .order_by('date')
    )
    return Response(trend)


# ---------- RatingByCompany Endpoints ----------
@api_view(['GET'])
@permission_classes([IsAuthenticated])
def top_rated_companies(request):
    return Response(
        RatingByCompany.objects.filter(rating__gte=4).values('company', 'rating').order_by('-rating')
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def average_rating_by_company(request):
    return Response(
        RatingByCompany.objects.values('company').annotate(avg_rating=Avg('rating')).order_by('-avg_rating')
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def most_demanded_jobs(request):
    result = (
        JobOffer.objects.values('normalized_title')
        .annotate(total=Count('id'))
        .order_by('-total')[:10]
    )
    return Response(result)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def most_required_skills(request):
    title = request.GET.get('title')
    if not title:
        return Response({"error": "Missing title"}, status=400)
    offers = JobOffer.objects.filter(normalized_title__iexact=title)
    all_skills = []
    for offer in offers:
        if offer.skills:
            all_skills += offer.skills
    top_skills = {s: all_skills.count(s) for s in set(all_skills)}
    return Response(sorted(top_skills.items(), key=lambda x: -x[1]))


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def job_offers_by_company(request):
    title = request.GET.get('title')
    if not title:
        return Response({"error": "Missing title"}, status=400)
    result = (
        JobOffer.objects.filter(normalized_title__iexact=title)
        .values('company')
        .annotate(total=Count('id'))
        .order_by('-total')
    )
    return Response(result)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def best_paying_job_offers(request):
    return Response(
        JobOffer.objects.filter(salary_max__isnull=False)
        .values('job_title', 'company', 'salary_max')
        .order_by('-salary_max')[:10]
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def jobs_by_region(request):
    title = request.GET.get('title')
    if not title:
        return Response({"error": "Missing title"}, status=400)
    return Response(
        JobOffer.objects.filter(normalized_title__iexact=title)
        .values('location')
        .annotate(count=Count('id'))
        .order_by('-count')
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def salary_comparison_by_region(request):
    title = request.GET.get('title')
    if not title:
        return Response({"error": "Missing title"}, status=400)
    return Response(
        JobOffer.objects.filter(normalized_title__iexact=title, salary_max__isnull=False)
        .values('location')
        .annotate(avg_salary=Avg('salary_max'))
        .order_by('-avg_salary')
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def contract_type_distribution(request):
    title = request.GET.get('title')
    if not title:
        return Response({"error": "Missing title"}, status=400)
    return Response(
        JobOffer.objects.filter(normalized_title__iexact=title)
        .values('contract_type')
        .annotate(count=Count('id'))
        .order_by('-count')
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def google_trends_growth(request):
    keyword = request.GET.get('keyword')
    country = request.GET.get('country')
    if not keyword or not country:
        return Response({"error": "Missing parameters"}, status=400)
    result = (
        GoogleTrends.objects.filter(keyword__iexact=keyword, country__iexact=country)
        .values('date')
        .annotate(avg_interest=Avg('interest'))
        .order_by('date')
    )
    return Response(result)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def peak_interest_date(request):
    keyword = request.GET.get('keyword')
    if not keyword:
        return Response({"error": "Missing keyword"}, status=400)
    result = (
        GoogleTrends.objects.filter(keyword__iexact=keyword)
        .values('date')
        .annotate(avg_interest=Avg('interest'))
        .order_by('-avg_interest')
    )
    return Response(result)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def top_countries_for_keyword(request):
    keyword = request.GET.get('keyword')
    if not keyword:
        return Response({"error": "Missing keyword"}, status=400)
    result = (
        GoogleTrends.objects.filter(keyword__iexact=keyword)
        .values('country')
        .annotate(avg_interest=Avg('interest'))
        .order_by('-avg_interest')
    )
    return Response(result)


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def top_github_projects(request):
    return Response(
        GithubProjects.objects.values('name', 'stars_period')
        .order_by('-stars_period')[:10]
    )


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def language_popularity(request):
    return Response(
        GithubProjects.objects.values('language')
        .annotate(total=Count('id'))
        .order_by('-total')
    )
