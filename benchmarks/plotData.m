fid = fopen('bigDataResults.txt');
line = fgetl(fid);
nums = [];
times = [];
while ischar(line)
    if startsWith(line, 'Total')
        break;
    end
    num = strsplit(line);
    num = char(num(2));
    num = strsplit(num, '/');
    num = str2num(char(num(1)));
    nums = [nums; num];
    
    line = fgetl(fid);
    time = strsplit(line, ':');
    time = char(time(2));
    time = strtrim(time);
    time = str2num(time(1:end-2));
    times = [times; time];
    
    line = fgetl(fid);
end
figure();
plot(nums, times);
grid on;

    