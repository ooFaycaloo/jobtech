from django.urls import path
from rest_framework.routers import DefaultRouter
from .views import (
    RegisterView,
    JobOfferViewSet,
    SalaryByCountryViewSet,
    RatingByCompanyViewSet,
    GoogleTrendsViewSet,
    GithubProjectsViewSet,

    salary_stats,
    top_jobs_by_salary,
    top_country_for_job,
    salary_trend_over_time,

    top_rated_companies,
    average_rating_by_company,

    most_demanded_jobs,
    most_required_skills,
    job_offers_by_company,
    best_paying_job_offers,
    jobs_by_region,
    salary_comparison_by_region,
    contract_type_distribution,

    google_trends_growth,
    peak_interest_date,
    top_countries_for_keyword,

    top_github_projects,
    language_popularity,
)

router = DefaultRouter()
router.register(r'joboffers-data', JobOfferViewSet)
router.register(r'salary-data', SalaryByCountryViewSet)
router.register(r'ratings-data', RatingByCompanyViewSet)
router.register(r'trends-data', GoogleTrendsViewSet)
router.register(r'github-data', GithubProjectsViewSet)

urlpatterns = router.urls + [

    path('register/', RegisterView.as_view(), name='register'),

    path('salary/stats/', salary_stats, name='salary_stats'),
    path('salary/top_jobs/', top_jobs_by_salary, name='top_jobs_by_salary'),
    path('salary/top_countries/', top_country_for_job, name='top_country_for_job'),
    path('salary/trends/', salary_trend_over_time, name='salary_trend'),

    path('ratings/top/', top_rated_companies, name='top_rated_companies'),
    path('ratings/average/', average_rating_by_company, name='average_rating'),

    path('joboffers/most_demanded/', most_demanded_jobs, name='most_demanded_jobs'),
    path('joboffers/skills/', most_required_skills, name='most_required_skills'),
    path('joboffers/by_company/', job_offers_by_company, name='job_offers_by_company'),
    path('joboffers/best_salary/', best_paying_job_offers, name='best_paying_job_offers'),
    path('joboffers/by_region/', jobs_by_region, name='jobs_by_region'),
    path('joboffers/salary_by_region/', salary_comparison_by_region, name='salary_comparison_by_region'),
    path('joboffers/contracts/', contract_type_distribution, name='contract_type_distribution'),

    path('trends/growth/', google_trends_growth, name='google_trends_growth'),
    path('trends/peak/', peak_interest_date, name='peak_interest_date'),
    path('trends/top_countries/', top_countries_for_keyword, name='top_countries_for_keyword'),

    path('github/top/', top_github_projects, name='top_github_projects'),
    path('github/languages/', language_popularity, name='language_popularity'),
]
