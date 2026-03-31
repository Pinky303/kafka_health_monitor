terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region"     { default = "ap-south-1" }
variable "hospital_name"  { default = "CityHospital" }
variable "doctor_email"   { description = "Doctor email for alerts" }
variable "doctor_phone"   { description = "Doctor phone for SMS alerts (+91...)" }
