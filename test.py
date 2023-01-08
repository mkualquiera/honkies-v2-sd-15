import argparse
import glob
import multiprocessing
import os
import sys
import time
from itertools import islice
from math import remainder
from types import SimpleNamespace

import k_diffusion as K
import numpy as np
import torch
from einops import rearrange
from omegaconf import OmegaConf
from PIL import Image
from torch import nn
from torchvision.utils import make_grid
from tqdm import tqdm, trange

from ldm.models.diffusion.ddim import DDIMSampler
from ldm.models.diffusion.plms import PLMSSampler
from ldm.util import instantiate_from_config
