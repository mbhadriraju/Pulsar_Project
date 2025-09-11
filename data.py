import numpy as np
import matplotlib.pyplot as plt
import psrsigsim as pss
from psrqpy import QueryATNF
import random as rd
import numpy as np
import warnings

def zscore(x):
    return (x - np.mean(x)) / np.std(x)

warnings.filterwarnings("ignore")

atnf_params = [
    'F0', 'P0', 'DM', 'W50', 'W10', 'S400', 'S1400', 'S2000',
    'RAJ', 'DECJ', 'Age', 'BSurf', 'Dist', 'Binary'
]

ism_fold = pss.ism.ISM()
tscope = pss.telescope.telescope.GBT()
obslen = 50
df = QueryATNF(params=atnf_params).pandas
valid_idx = df[(df['P0'].notna()) & (df['DM'].notna()) & (df['P0'] <= 0.01)].index
ref_freq = 1400.0
f0 = 1400
bw = 800
Nf = 32
sublen = 60.0

def red_and_white_noise(r, n, num_chan, intensity_red, intensity_white):
    arr = np.zeros(n)
    noise_arr = np.zeros((num_chan, n))
    sig = rd.gauss(0, 1)
    arr[0] = sig
    for i in range(1, n):
        sig = (sig * r) + (rd.gauss(0, 1) * np.sqrt(1 - r**2))
        arr[i] = sig
    for i in range(1, num_chan + 1):
        noise_arr[i - 1] = arr * (1 / (i ** 2))
    red_noise_arr = noise_arr * intensity_red
    white_noise_arr = np.random.normal(loc=0, scale=1, size=(num_chan, n)) * intensity_white
    return red_noise_arr + white_noise_arr

N = 10000
tensor = np.zeros((N, 2, 32, 1024), dtype=np.float32)

for j in range(N):
    try:
        idx = rd.choice(valid_idx)

        period = df['P0'][idx]
        f_samp = (1.0 / period) * 1024 * 1e-6
        smean = df['S1400'][idx] if not np.isnan(df['S1400'][idx]) else 0.005
        psr_name = df['JNAME'][idx] if 'JNAME' in df.columns and not df.isna(df['JNAME'][idx]) else f"Pulsar_{idx}"
        specidx = rd.uniform(-1, -2)

        signal_fold = pss.signal.FilterBankSignal(
            fcent=f0, bandwidth=bw, Nsubband=Nf, sample_rate=f_samp, sublen=sublen, fold=True
        )

        l = rd.randint(1, 10)
        p1 = rd.uniform(0.2, 0.8)
        p2 = rd.uniform(0.2, 0.8) if l > 5 else None

        peaks, widths, amps = [], [], []
        for i in range(l):
            widths.append(rd.uniform(0.004, 0.05))
            center = p2 if (i >= l // 2 and p2) else p1
            peaks.append(rd.uniform(max(0, center - 0.05), min(1, center + 0.05)))
            amps.append(rd.uniform(0, 1))

        peaks, widths, amps = map(np.array, (peaks, widths, amps))
        amps /= amps.max()

        gauss_prof = pss.pulsar.GaussProfile(peak=peaks, width=widths, amp=amps)
        gauss_prof.init_profiles(1024, Nchan=Nf)

        
        pulsar_fold = pss.pulsar.Pulsar(period, smean, profiles=gauss_prof,
                                        name=psr_name, specidx=specidx, ref_freq=ref_freq)
        

        fd = [rd.uniform(-0.0002, 0.0002) for i in range(5)]
        ism_fold.scatter_broaden(signal_fold, rd.uniform(1e-5, 1e-4), ref_freq, convolve=True, pulsar=pulsar_fold)
        pulsar_fold.make_pulses(signal_fold, tobs=obslen)
        ism_fold.FD_shift(signal_fold, fd)
        tscope.observe(signal_fold, pulsar_fold, system="Lband_GUPPI", noise=False)
        signal_fold_data = signal_fold.data.copy()
        denoise_data = signal_fold.data.copy()

        intent_red = rd.uniform(0.01, 0.4)
        intent_white = rd.uniform(0.01, 0.4)

        noise_arr = red_and_white_noise(rd.uniform(0.9, 1), len(signal_fold.data[0]), Nf,
                                        np.mean(signal_fold.data[0]) * intent_red,
                                        np.mean(signal_fold.data[0]) * intent_white)

        signal_fold_data += noise_arr

        signal_fold_data = zscore(signal_fold_data)
        denoise_data = zscore(denoise_data)
        tensor[j] = [signal_fold_data, denoise_data]
    except IndexError:
        j -= 1
        continue

np.save('sim_data.npy', tensor)

time = np.linspace(0, signal_fold.sublen, signal_fold.data.shape[1])
plt.imshow(signal_fold_data, aspect='auto', interpolation='nearest', origin='lower')
plt.ylabel("Frequency [MHz]")
plt.xlabel("Time [s]")
plt.colorbar(label="Intensity")
plt.title("Dynamic Spectrum with Noise + Scattering + Bandpass")
plt.show()